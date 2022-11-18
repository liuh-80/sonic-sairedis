#include "Syncd.h"
#include "VidManager.h"
#include "NotificationHandler.h"
#include "Workaround.h"
#include "ComparisonLogic.h"
#include "HardReiniter.h"
#include "RedisClient.h"
#include "RequestShutdown.h"
#include "WarmRestartTable.h"
#include "ContextConfigContainer.h"
#include "BreakConfigParser.h"
#include "RedisNotificationProducer.h"
#include "ZeroMQNotificationProducer.h"
#include "WatchdogScope.h"

#include "sairediscommon.h"

#include "swss/logger.h"
#include "swss/select.h"
#include "swss/tokenize.h"
#include "swss/notificationproducer.h"

#include "meta/sai_serialize.h"
#include "meta/ZeroMQSelectableChannel.h"
#include "meta/RedisSelectableChannel.h"
#include "meta/PerformanceIntervalTimer.h"

#include "vslib/saivs.h"

#include <unistd.h>
#include <inttypes.h>

#include <iterator>
#include <algorithm>

#define DEF_SAI_WARM_BOOT_DATA_FILE "/var/warmboot/sai-warmboot.bin"

using namespace syncd;
using namespace saimeta;
using namespace sairediscommon;
using namespace std::placeholders;

Syncd::Syncd(
        _In_ std::shared_ptr<sairedis::SaiInterface> vendorSai,
        _In_ std::shared_ptr<CommandLineOptions> cmd,
        _In_ bool isWarmStart):
    m_commandLineOptions(cmd),
    m_isWarmStart(isWarmStart),
    m_firstInitWasPerformed(false),
    m_asicInitViewMode(false), // by default we are in APPLY view mode
    m_vendorSai(vendorSai),
    m_veryFirstRun(false),
    m_enableSyncMode(false),
    m_timerWatchdog(30 * 1000000) // watch for executions over 30 seconds
{
    SWSS_LOG_ENTER();

    setSaiApiLogLevel();

    SWSS_LOG_NOTICE("command line: %s", m_commandLineOptions->getCommandLineString().c_str());

    auto ccc = sairedis::ContextConfigContainer::loadFromFile(m_commandLineOptions->m_contextConfig.c_str());

    m_contextConfig = ccc->get(m_commandLineOptions->m_globalContext);

    if (m_contextConfig == nullptr)
    {
        SWSS_LOG_THROW("no context config defined at global context %u", m_commandLineOptions->m_globalContext);
    }

    if (m_contextConfig->m_zmqEnable && m_commandLineOptions->m_enableSyncMode)
    {
        SWSS_LOG_NOTICE("disabling command line sync mode, since context zmq enabled");

        m_commandLineOptions->m_enableSyncMode = false;

        m_commandLineOptions->m_redisCommunicationMode = SAI_REDIS_COMMUNICATION_MODE_ZMQ_SYNC;
    }

    if (m_commandLineOptions->m_enableSyncMode)
    {
        SWSS_LOG_WARN("enable sync mode is deprecated, please use communication mode, FORCING redis sync mode");

        m_enableSyncMode = true;

        m_contextConfig->m_zmqEnable = false;

        m_commandLineOptions->m_redisCommunicationMode = SAI_REDIS_COMMUNICATION_MODE_REDIS_SYNC;
    }

    if (m_commandLineOptions->m_redisCommunicationMode == SAI_REDIS_COMMUNICATION_MODE_ZMQ_SYNC)
    {
        SWSS_LOG_NOTICE("zmq sync mode enabled via cmd line");

        m_contextConfig->m_zmqEnable = true;

        m_enableSyncMode = true;
    }

    m_manager = std::make_shared<FlexCounterManager>(m_vendorSai, m_contextConfig->m_dbCounters);

    loadProfileMap();

    m_profileIter = m_profileMap.begin();

    // we need STATE_DB ASIC_DB and COUNTERS_DB

    m_dbAsic = std::make_shared<swss::DBConnector>(m_contextConfig->m_dbAsic, 0);
    m_mdioIpcServer = std::make_shared<MdioIpcServer>(m_vendorSai, m_commandLineOptions->m_globalContext);

    if (m_contextConfig->m_zmqEnable)
    {
        m_notifications = std::make_shared<ZeroMQNotificationProducer>(m_contextConfig->m_zmqNtfEndpoint);

        SWSS_LOG_NOTICE("zmq enabled, forcing sync mode");

        m_enableSyncMode = true;

        m_selectableChannel = std::make_shared<sairedis::ZeroMQSelectableChannel>(m_contextConfig->m_zmqEndpoint);
    }
    else
    {
        m_notifications = std::make_shared<RedisNotificationProducer>(m_contextConfig->m_dbAsic);

        m_enableSyncMode = m_commandLineOptions->m_redisCommunicationMode == SAI_REDIS_COMMUNICATION_MODE_REDIS_SYNC;

        bool modifyRedis = m_enableSyncMode ? false : true;

        m_selectableChannel = std::make_shared<sairedis::RedisSelectableChannel>(
                m_dbAsic,
                ASIC_STATE_TABLE,
                REDIS_TABLE_GETRESPONSE,
                TEMP_PREFIX,
                modifyRedis);
    }

    m_client = std::make_shared<RedisClient>(m_dbAsic);

    m_processor = std::make_shared<NotificationProcessor>(m_notifications, m_client, std::bind(&Syncd::syncProcessNotification, this, _1));
    m_handler = std::make_shared<NotificationHandler>(m_processor);

    m_sn.onFdbEvent = std::bind(&NotificationHandler::onFdbEvent, m_handler.get(), _1, _2);
    m_sn.onNatEvent = std::bind(&NotificationHandler::onNatEvent, m_handler.get(), _1, _2);
    m_sn.onPortStateChange = std::bind(&NotificationHandler::onPortStateChange, m_handler.get(), _1, _2);
    m_sn.onQueuePfcDeadlock = std::bind(&NotificationHandler::onQueuePfcDeadlock, m_handler.get(), _1, _2);
    m_sn.onSwitchShutdownRequest = std::bind(&NotificationHandler::onSwitchShutdownRequest, m_handler.get(), _1);
    m_sn.onSwitchStateChange = std::bind(&NotificationHandler::onSwitchStateChange, m_handler.get(), _1, _2);
    m_sn.onBfdSessionStateChange = std::bind(&NotificationHandler::onBfdSessionStateChange, m_handler.get(), _1, _2);

    m_handler->setSwitchNotifications(m_sn.getSwitchNotifications());

    m_restartQuery = std::make_shared<swss::NotificationConsumer>(m_dbAsic.get(), SYNCD_NOTIFICATION_CHANNEL_RESTARTQUERY);

    // TODO to be moved to ASIC_DB
    m_dbFlexCounter = std::make_shared<swss::DBConnector>(m_contextConfig->m_dbFlex, 0);
    m_flexCounter = std::make_shared<swss::ConsumerTable>(m_dbFlexCounter.get(), FLEX_COUNTER_TABLE);
    m_flexCounterGroup = std::make_shared<swss::ConsumerTable>(m_dbFlexCounter.get(), FLEX_COUNTER_GROUP_TABLE);

    m_switchConfigContainer = std::make_shared<sairedis::SwitchConfigContainer>();
    m_redisVidIndexGenerator = std::make_shared<sairedis::RedisVidIndexGenerator>(m_dbAsic, REDIS_KEY_VIDCOUNTER);

    m_virtualObjectIdManager =
        std::make_shared<sairedis::VirtualObjectIdManager>(
                m_commandLineOptions->m_globalContext,
                m_switchConfigContainer,
                m_redisVidIndexGenerator);

    // TODO move to syncd object
    m_translator = std::make_shared<VirtualOidTranslator>(m_client, m_virtualObjectIdManager,  vendorSai);

    m_processor->m_translator = m_translator; // TODO as param

    m_veryFirstRun = isVeryFirstRun();

    performStartupLogic();

    m_smt.profileGetValue = std::bind(&Syncd::profileGetValue, this, _1, _2);
    m_smt.profileGetNextValue = std::bind(&Syncd::profileGetNextValue, this, _1, _2, _3);

    m_test_services = m_smt.getServiceMethodTable();

    sai_status_t status = vendorSai->initialize(0, &m_test_services);

    if (status != SAI_STATUS_SUCCESS)
    {
        SWSS_LOG_ERROR("FATAL: failed to sai_api_initialize: %s",
                sai_serialize_status(status).c_str());

        abort();
    }

    m_breakConfig = BreakConfigParser::parseBreakConfig(m_commandLineOptions->m_breakConfig);

    SWSS_LOG_NOTICE("syncd started");
}

Syncd::~Syncd()
{
    SWSS_LOG_ENTER();

    // empty
}

void Syncd::performStartupLogic()
{
    SWSS_LOG_ENTER();

    // ignore warm logic here if syncd starts in fast-boot or Mellanox fastfast boot mode

    if (m_isWarmStart && m_commandLineOptions->m_startType != SAI_START_TYPE_FASTFAST_BOOT && m_commandLineOptions->m_startType != SAI_START_TYPE_FAST_BOOT)
    {
        SWSS_LOG_WARN("override command line startType=%s via SAI_START_TYPE_WARM_BOOT",
                CommandLineOptions::startTypeToString(m_commandLineOptions->m_startType).c_str());

        m_commandLineOptions->m_startType = SAI_START_TYPE_WARM_BOOT;
    }

    if (m_commandLineOptions->m_startType == SAI_START_TYPE_WARM_BOOT)
    {
        const char *warmBootReadFile = profileGetValue(0, SAI_KEY_WARM_BOOT_READ_FILE);

        SWSS_LOG_NOTICE("using warmBootReadFile: '%s'", warmBootReadFile);

        if (warmBootReadFile == NULL || access(warmBootReadFile, F_OK) == -1)
        {
            SWSS_LOG_WARN("user requested warmStart but warmBootReadFile is not specified or not accessible, forcing cold start");

            m_commandLineOptions->m_startType = SAI_START_TYPE_COLD_BOOT;
        }
    }

    if (m_commandLineOptions->m_startType == SAI_START_TYPE_WARM_BOOT && m_veryFirstRun)
    {
        SWSS_LOG_WARN("warm start requested, but this is very first syncd start, forcing cold start");

        /*
         * We force cold start since if it's first run then redis db is not
         * complete so redis asic view will not reflect warm boot asic state,
         * if this happen then orch agent needs to be restarted as well to
         * repopulate asic view.
         */

        m_commandLineOptions->m_startType = SAI_START_TYPE_COLD_BOOT;
    }

    if (m_commandLineOptions->m_startType == SAI_START_TYPE_FASTFAST_BOOT)
    {
        /*
         * Mellanox SAI requires to pass SAI_WARM_BOOT as SAI_BOOT_KEY
         * to start 'fastfast'
         */

        m_profileMap[SAI_KEY_BOOT_TYPE] = std::to_string(SAI_START_TYPE_WARM_BOOT);
    }
    else
    {
        m_profileMap[SAI_KEY_BOOT_TYPE] = std::to_string(m_commandLineOptions->m_startType); // number value is needed
    }
}

bool Syncd::getAsicInitViewMode() const
{
    SWSS_LOG_ENTER();

    return m_asicInitViewMode;
}

void Syncd::setAsicInitViewMode(
        _In_ bool enable)
{
    SWSS_LOG_ENTER();

    m_asicInitViewMode = enable;
}

bool Syncd::isInitViewMode() const
{
    SWSS_LOG_ENTER();

    return m_asicInitViewMode && m_commandLineOptions->m_enableTempView;
}

void Syncd::processEvent(
        _In_ sairedis::SelectableChannel& consumer)
{
    SWSS_LOG_ENTER();

    std::lock_guard<std::mutex> lock(m_mutex);

    do
    {
        swss::KeyOpFieldsValuesTuple kco;

        /*
         * In init mode we put all data to TEMP view and we snoop.  We need
         * to specify temporary view prefix in consumer since consumer puts
         * data to redis db.
         */

        consumer.pop(kco, isInitViewMode());

        processSingleEvent(kco);
    }
    while (!consumer.empty());
}

sai_status_t Syncd::processSingleEvent(
        _In_ const swss::KeyOpFieldsValuesTuple &kco)
{
    SWSS_LOG_ENTER();

    auto& key = kfvKey(kco);
    auto& op = kfvOp(kco);

    SWSS_LOG_INFO("key: %s op: %s", key.c_str(), op.c_str());

    if (key.length() == 0)
    {
        SWSS_LOG_DEBUG("no elements in m_buffer");

        return SAI_STATUS_SUCCESS;
    }

    WatchdogScope ws(m_timerWatchdog, op + ":" + key, &kco);

    if (op == REDIS_ASIC_STATE_COMMAND_CREATE)
        return processQuadEvent(SAI_COMMON_API_CREATE, kco);

    if (op == REDIS_ASIC_STATE_COMMAND_REMOVE)
        return processQuadEvent(SAI_COMMON_API_REMOVE, kco);

    if (op == REDIS_ASIC_STATE_COMMAND_SET)
        return processQuadEvent(SAI_COMMON_API_SET, kco);

    if (op == REDIS_ASIC_STATE_COMMAND_GET)
        return processQuadEvent(SAI_COMMON_API_GET, kco);

    if (op == REDIS_ASIC_STATE_COMMAND_BULK_CREATE)
        return processBulkQuadEvent(SAI_COMMON_API_BULK_CREATE, kco);

    if (op == REDIS_ASIC_STATE_COMMAND_BULK_REMOVE)
        return processBulkQuadEvent(SAI_COMMON_API_BULK_REMOVE, kco);

    if (op == REDIS_ASIC_STATE_COMMAND_BULK_SET)
        return processBulkQuadEvent(SAI_COMMON_API_BULK_SET, kco);

    if (op == REDIS_ASIC_STATE_COMMAND_NOTIFY)
        return processNotifySyncd(kco);

    if (op == REDIS_ASIC_STATE_COMMAND_GET_STATS)
        return processGetStatsEvent(kco);

    if (op == REDIS_ASIC_STATE_COMMAND_CLEAR_STATS)
        return processClearStatsEvent(kco);

    if (op == REDIS_ASIC_STATE_COMMAND_FLUSH)
        return processFdbFlush(kco);

    if (op == REDIS_ASIC_STATE_COMMAND_ATTR_CAPABILITY_QUERY)
        return processAttrCapabilityQuery(kco);

    if (op == REDIS_ASIC_STATE_COMMAND_ATTR_ENUM_VALUES_CAPABILITY_QUERY)
        return processAttrEnumValuesCapabilityQuery(kco);

    if (op == REDIS_ASIC_STATE_COMMAND_OBJECT_TYPE_GET_AVAILABILITY_QUERY)
        return processObjectTypeGetAvailabilityQuery(kco);

    SWSS_LOG_THROW("event op '%s' is not implemented, FIXME", op.c_str());
}

sai_status_t Syncd::processAttrCapabilityQuery(
        _In_ const swss::KeyOpFieldsValuesTuple &kco)
{
    SWSS_LOG_ENTER();

    auto& strSwitchVid = kfvKey(kco);

    sai_object_id_t switchVid;
    sai_deserialize_object_id(strSwitchVid, switchVid);

    sai_object_id_t switchRid = m_translator->translateVidToRid(switchVid);

    auto& values = kfvFieldsValues(kco);

    if (values.size() != 2)
    {
        SWSS_LOG_ERROR("Invalid input: expected 2 arguments, received %zu", values.size());

        m_selectableChannel->set(sai_serialize_status(SAI_STATUS_INVALID_PARAMETER), {}, REDIS_ASIC_STATE_COMMAND_ATTR_CAPABILITY_RESPONSE);

        return SAI_STATUS_INVALID_PARAMETER;
    }

    sai_object_type_t objectType;
    sai_deserialize_object_type(fvValue(values[0]), objectType);

    sai_attr_id_t attrId;
    sai_deserialize_attr_id(fvValue(values[1]), attrId);

    sai_attr_capability_t capability;

    sai_status_t status = m_vendorSai->queryAttributeCapability(switchRid, objectType, attrId, &capability);

    std::vector<swss::FieldValueTuple> entry;

    if (status == SAI_STATUS_SUCCESS)
    {
        entry =
        {
            swss::FieldValueTuple("CREATE_IMPLEMENTED", (capability.create_implemented ? "true" : "false")),
            swss::FieldValueTuple("SET_IMPLEMENTED",    (capability.set_implemented    ? "true" : "false")),
            swss::FieldValueTuple("GET_IMPLEMENTED",    (capability.get_implemented    ? "true" : "false"))
        };

        SWSS_LOG_INFO("Sending response: create_implemented:%d, set_implemented:%d, get_implemented:%d",
            capability.create_implemented, capability.set_implemented, capability.get_implemented);
    }

    m_selectableChannel->set(sai_serialize_status(status), entry, REDIS_ASIC_STATE_COMMAND_ATTR_CAPABILITY_RESPONSE);

    return status;
}

sai_status_t Syncd::processAttrEnumValuesCapabilityQuery(
        _In_ const swss::KeyOpFieldsValuesTuple &kco)
{
    SWSS_LOG_ENTER();

    auto& strSwitchVid = kfvKey(kco);

    sai_object_id_t switchVid;
    sai_deserialize_object_id(strSwitchVid, switchVid);

    sai_object_id_t switchRid = m_translator->translateVidToRid(switchVid);

    auto& values = kfvFieldsValues(kco);

    if (values.size() != 3)
    {
        SWSS_LOG_ERROR("Invalid input: expected 3 arguments, received %zu", values.size());

        m_selectableChannel->set(sai_serialize_status(SAI_STATUS_INVALID_PARAMETER), {}, REDIS_ASIC_STATE_COMMAND_ATTR_ENUM_VALUES_CAPABILITY_RESPONSE);

        return SAI_STATUS_INVALID_PARAMETER;
    }

    sai_object_type_t objectType;
    sai_deserialize_object_type(fvValue(values[0]), objectType);

    sai_attr_id_t attrId;
    sai_deserialize_attr_id(fvValue(values[1]), attrId);

    uint32_t list_size = std::stoi(fvValue(values[2]));

    std::vector<int32_t> enum_capabilities_list(list_size);

    sai_s32_list_t enumCapList;

    enumCapList.count = list_size;
    enumCapList.list = enum_capabilities_list.data();

    sai_status_t status = m_vendorSai->queryAattributeEnumValuesCapability(switchRid, objectType, attrId, &enumCapList);

    std::vector<swss::FieldValueTuple> entry;

    if (status == SAI_STATUS_SUCCESS)
    {
        std::vector<std::string> vec;
        std::transform(enumCapList.list, enumCapList.list + enumCapList.count,
                std::back_inserter(vec), [](auto&e) { return std::to_string(e); });

        std::ostringstream join;
        std::copy(vec.begin(), vec.end(), std::ostream_iterator<std::string>(join, ","));

        auto strCap = join.str();

        entry =
        {
            swss::FieldValueTuple("ENUM_CAPABILITIES", strCap),
            swss::FieldValueTuple("ENUM_COUNT", std::to_string(enumCapList.count))
        };

        SWSS_LOG_DEBUG("Sending response: capabilities = '%s', count = %d", strCap.c_str(), enumCapList.count);
    }

    m_selectableChannel->set(sai_serialize_status(status), entry, REDIS_ASIC_STATE_COMMAND_ATTR_ENUM_VALUES_CAPABILITY_RESPONSE);

    return status;
}

sai_status_t Syncd::processObjectTypeGetAvailabilityQuery(
    _In_ const swss::KeyOpFieldsValuesTuple &kco)
{
    SWSS_LOG_ENTER();

    auto& strSwitchVid = kfvKey(kco);

    sai_object_id_t switchVid;
    sai_deserialize_object_id(strSwitchVid, switchVid);

    const sai_object_id_t switchRid = m_translator->translateVidToRid(switchVid);

    std::vector<swss::FieldValueTuple> values = kfvFieldsValues(kco);

    // Syncd needs to pop the object type off the end of the list in order to
    // retrieve the attribute list

    sai_object_type_t objectType;
    sai_deserialize_object_type(fvValue(values.back()), objectType);

    values.pop_back();

    SaiAttributeList list(objectType, values, false);

    sai_attribute_t *attr_list = list.get_attr_list();

    uint32_t attr_count = list.get_attr_count();

    m_translator->translateVidToRid(objectType, attr_count, attr_list);

    uint64_t count;

    sai_status_t status = m_vendorSai->objectTypeGetAvailability(
            switchRid,
            objectType,
            attr_count,
            attr_list,
            &count);

    std::vector<swss::FieldValueTuple> entry;

    if (status == SAI_STATUS_SUCCESS)
    {
        entry.push_back(swss::FieldValueTuple("OBJECT_COUNT", std::to_string(count)));

        SWSS_LOG_DEBUG("Sending response: count = %lu", count);
    }

    m_selectableChannel->set(sai_serialize_status(status), entry, REDIS_ASIC_STATE_COMMAND_OBJECT_TYPE_GET_AVAILABILITY_RESPONSE);

    return status;
}

sai_status_t Syncd::processFdbFlush(
        _In_ const swss::KeyOpFieldsValuesTuple &kco)
{
    SWSS_LOG_ENTER();

    auto& key = kfvKey(kco);
    auto strSwitchVid = key.substr(key.find(":") + 1);

    sai_object_id_t switchVid;
    sai_deserialize_object_id(strSwitchVid, switchVid);

    sai_object_id_t switchRid = m_translator->translateVidToRid(switchVid);

    auto& values = kfvFieldsValues(kco);

    for (const auto &v: values)
    {
        SWSS_LOG_DEBUG("attr: %s: %s", fvField(v).c_str(), fvValue(v).c_str());
    }

    SaiAttributeList list(SAI_OBJECT_TYPE_FDB_FLUSH, values, false);
    SaiAttributeList vidlist(SAI_OBJECT_TYPE_FDB_FLUSH, values, false);

    /*
     * Attribute list can't be const since we will use it to translate VID to
     * RID in place.
     */

    sai_attribute_t *attr_list = list.get_attr_list();
    uint32_t attr_count = list.get_attr_count();

    m_translator->translateVidToRid(SAI_OBJECT_TYPE_FDB_FLUSH, attr_count, attr_list);

    sai_status_t status = m_vendorSai->flushFdbEntries(switchRid, attr_count, attr_list);

    m_selectableChannel->set(sai_serialize_status(status), {} , REDIS_ASIC_STATE_COMMAND_FLUSHRESPONSE);

    if (status == SAI_STATUS_SUCCESS)
    {
        SWSS_LOG_NOTICE("fdb flush succeeded, updating redis database");

        // update database right after fdb flush success (not in notification)
        // build artificial notification here to reuse code

        sai_fdb_flush_entry_type_t type = SAI_FDB_FLUSH_ENTRY_TYPE_DYNAMIC;

        sai_object_id_t bvId = SAI_NULL_OBJECT_ID;
        sai_object_id_t bridgePortId = SAI_NULL_OBJECT_ID;

        attr_list = vidlist.get_attr_list();
        attr_count = vidlist.get_attr_count();

        for (uint32_t i = 0; i < attr_count; i++)
        {
            switch (attr_list[i].id)
            {
                case SAI_FDB_FLUSH_ATTR_BRIDGE_PORT_ID:
                    bridgePortId = attr_list[i].value.oid;
                    break;

                case SAI_FDB_FLUSH_ATTR_BV_ID:
                    bvId = attr_list[i].value.oid;
                    break;

                case SAI_FDB_FLUSH_ATTR_ENTRY_TYPE:
                    type = (sai_fdb_flush_entry_type_t)attr_list[i].value.s32;
                    break;

                default:
                    SWSS_LOG_ERROR("unsupported attribute: %d, skipping", attr_list[i].id);
                    break;
            }
        }

        m_client->processFlushEvent(switchVid, bridgePortId, bvId, type);
    }

    return status;
}

sai_status_t Syncd::processClearStatsEvent(
        _In_ const swss::KeyOpFieldsValuesTuple &kco)
{
    SWSS_LOG_ENTER();

    const std::string &key = kfvKey(kco);

    sai_object_meta_key_t metaKey;
    sai_deserialize_object_meta_key(key, metaKey);

    if (isInitViewMode() && m_createdInInitView.find(metaKey.objectkey.key.object_id) != m_createdInInitView.end())
    {
        SWSS_LOG_WARN("CLEAR STATS api can't be used on %s since it's created in INIT_VIEW mode", key.c_str());

        sai_status_t status = SAI_STATUS_INVALID_OBJECT_ID;

        m_selectableChannel->set(sai_serialize_status(status), {}, REDIS_ASIC_STATE_COMMAND_GETRESPONSE);

        return status;
    }

    if (!m_translator->tryTranslateVidToRid(metaKey))
    {
        SWSS_LOG_WARN("VID to RID translation failure: %s", key.c_str());
        sai_status_t status = SAI_STATUS_INVALID_OBJECT_ID;
        m_selectableChannel->set(sai_serialize_status(status), {}, REDIS_ASIC_STATE_COMMAND_GETRESPONSE);
        return status;
    }

    auto info = sai_metadata_get_object_type_info(metaKey.objecttype);

    if (info->isnonobjectid)
    {
        SWSS_LOG_THROW("non object id not supported on clear stats: %s, FIXME", key.c_str());
    }

    std::vector<sai_stat_id_t> counter_ids;

    for (auto&v: kfvFieldsValues(kco))
    {
        int32_t val;
        sai_deserialize_enum(fvField(v), info->statenum, val);

        counter_ids.push_back(val);
    }

    auto status = m_vendorSai->clearStats(
            metaKey.objecttype,
            metaKey.objectkey.key.object_id,
            (uint32_t)counter_ids.size(),
            counter_ids.data());

    m_selectableChannel->set(sai_serialize_status(status), {}, REDIS_ASIC_STATE_COMMAND_GETRESPONSE);

    return status;
}

sai_status_t Syncd::processGetStatsEvent(
        _In_ const swss::KeyOpFieldsValuesTuple &kco)
{
    SWSS_LOG_ENTER();

    const std::string &key = kfvKey(kco);

    sai_object_meta_key_t metaKey;
    sai_deserialize_object_meta_key(key, metaKey);

    if (isInitViewMode() && m_createdInInitView.find(metaKey.objectkey.key.object_id) != m_createdInInitView.end())
    {
        SWSS_LOG_WARN("GET STATS api can't be used on %s since it's created in INIT_VIEW mode", key.c_str());

        sai_status_t status = SAI_STATUS_INVALID_OBJECT_ID;

        m_selectableChannel->set(sai_serialize_status(status), {}, REDIS_ASIC_STATE_COMMAND_GETRESPONSE);

        return status;
    }

    m_translator->translateVidToRid(metaKey);

    auto info = sai_metadata_get_object_type_info(metaKey.objecttype);

    if (info->isnonobjectid)
    {
        SWSS_LOG_THROW("non object id not supported on clear stats: %s, FIXME", key.c_str());
    }

    std::vector<sai_stat_id_t> counter_ids;

    for (auto&v: kfvFieldsValues(kco))
    {
        int32_t val;
        sai_deserialize_enum(fvField(v), info->statenum, val);

        counter_ids.push_back(val);
    }

    std::vector<uint64_t> result(counter_ids.size());

    auto status = m_vendorSai->getStats(
            metaKey.objecttype,
            metaKey.objectkey.key.object_id,
            (uint32_t)counter_ids.size(),
            counter_ids.data(),
            result.data());

    std::vector<swss::FieldValueTuple> entry;

    if (status != SAI_STATUS_SUCCESS)
    {
        SWSS_LOG_NOTICE("Getting stats error: %s", sai_serialize_status(status).c_str());
    }
    else
    {
        const auto& values = kfvFieldsValues(kco);

        for (size_t i = 0; i < values.size(); i++)
        {
            entry.emplace_back(fvField(values[i]), std::to_string(result[i]));
        }
    }

    m_selectableChannel->set(sai_serialize_status(status), entry, REDIS_ASIC_STATE_COMMAND_GETRESPONSE);

    return status;
}

sai_status_t Syncd::processBulkQuadEvent(
        _In_ sai_common_api_t api,
        _In_ const swss::KeyOpFieldsValuesTuple &kco)
{
    SWSS_LOG_ENTER();

    const std::string& key = kfvKey(kco); // objectType:count

    std::string strObjectType = key.substr(0, key.find(":"));

    sai_object_type_t objectType;
    sai_deserialize_object_type(strObjectType, objectType);

    const std::vector<swss::FieldValueTuple> &values = kfvFieldsValues(kco);

    std::vector<std::vector<swss::FieldValueTuple>> strAttributes;

    // field = objectId
    // value = attrid=attrvalue|...

    std::vector<std::string> objectIds;

    std::vector<std::shared_ptr<SaiAttributeList>> attributes;

    for (const auto &fvt: values)
    {
        std::string strObjectId = fvField(fvt);
        std::string joined = fvValue(fvt);

        // decode values

        auto v = swss::tokenize(joined, '|');

        objectIds.push_back(strObjectId);

        std::vector<swss::FieldValueTuple> entries; // attributes per object id

        for (size_t i = 0; i < v.size(); ++i)
        {
            const std::string item = v.at(i);

            auto start = item.find_first_of("=");

            auto field = item.substr(0, start);
            auto value = item.substr(start + 1);

            entries.emplace_back(field, value);
        }

        strAttributes.push_back(entries);

        // since now we converted this to proper list, we can extract attributes

        auto list = std::make_shared<SaiAttributeList>(objectType, entries, false);

        attributes.push_back(list);
    }

    SWSS_LOG_INFO("bulk %s executing with %zu items",
            strObjectType.c_str(),
            objectIds.size());

    if (isInitViewMode())
    {
        return processBulkQuadEventInInitViewMode(objectType, objectIds, api, attributes, strAttributes);
    }

    if (api != SAI_COMMON_API_BULK_GET)
    {
        // translate attributes for all objects

        for (auto &list: attributes)
        {
            sai_attribute_t *attr_list = list->get_attr_list();
            uint32_t attr_count = list->get_attr_count();

            m_translator->translateVidToRid(objectType, attr_count, attr_list);
        }
    }

    auto info = sai_metadata_get_object_type_info(objectType);

    if (info->isobjectid)
    {
        return processBulkOid(objectType, objectIds, api, attributes, strAttributes);
    }
    else
    {
        return processBulkEntry(objectType, objectIds, api, attributes, strAttributes);
    }
}

sai_status_t Syncd::processBulkQuadEventInInitViewMode(
        _In_ sai_object_type_t objectType,
        _In_ const std::vector<std::string>& objectIds,
        _In_ sai_common_api_t api,
        _In_ const std::vector<std::shared_ptr<saimeta::SaiAttributeList>>& attributes,
        _In_ const std::vector<std::vector<swss::FieldValueTuple>>& strAttributes)
{
    SWSS_LOG_ENTER();

    std::vector<sai_status_t> statuses(objectIds.size());

    for (auto &a: statuses)
    {
        a = SAI_STATUS_SUCCESS;
    }

    auto info = sai_metadata_get_object_type_info(objectType);

    switch (api)
    {
        case SAI_COMMON_API_BULK_CREATE:
        case SAI_COMMON_API_BULK_REMOVE:
        case SAI_COMMON_API_BULK_SET:

            if (info->isnonobjectid)
            {
                sendApiResponse(api, SAI_STATUS_SUCCESS, (uint32_t)statuses.size(), statuses.data());

                syncUpdateRedisBulkQuadEvent(api, statuses, objectType, objectIds, strAttributes);

                return SAI_STATUS_SUCCESS;
            }

            switch (objectType)
            {
                case SAI_OBJECT_TYPE_SWITCH:
                case SAI_OBJECT_TYPE_PORT:
                case SAI_OBJECT_TYPE_SCHEDULER_GROUP:
                case SAI_OBJECT_TYPE_INGRESS_PRIORITY_GROUP:

                    SWSS_LOG_THROW("%s is not supported in init view mode",
                            sai_serialize_object_type(objectType).c_str());

                default:

                    sendApiResponse(api, SAI_STATUS_SUCCESS, (uint32_t)statuses.size(), statuses.data());

                    syncUpdateRedisBulkQuadEvent(api, statuses, objectType, objectIds, strAttributes);

                    for (auto& str: objectIds)
                    {
                        sai_object_id_t objectVid;
                        sai_deserialize_object_id(str, objectVid);

                        // in init view mode insert every created object except switch

                        m_createdInInitView.insert(objectVid);
                    }

                    return SAI_STATUS_SUCCESS;
            }

        case SAI_COMMON_API_BULK_GET:
            SWSS_LOG_THROW("GET bulk api is not implemented in init view mode, FIXME");

        default:

            SWSS_LOG_THROW("common bulk api (%s) is not implemented in init view mode",
                    sai_serialize_common_api(api).c_str());
    }
}

sai_status_t Syncd::processBulkCreateEntry(
        _In_ sai_object_type_t objectType,
        _In_ const std::vector<std::string>& objectIds,
        _In_ const std::vector<std::shared_ptr<SaiAttributeList>>& attributes,
        _Out_ std::vector<sai_status_t>& statuses)
{
    SWSS_LOG_ENTER();
    sai_status_t status = SAI_STATUS_SUCCESS;

    uint32_t object_count = (uint32_t) objectIds.size();

    if (!object_count)
    {
        SWSS_LOG_ERROR("container with objectIds is empty in processBulkCreateEntry");
        return SAI_STATUS_FAILURE;
    }

    sai_bulk_op_error_mode_t mode = SAI_BULK_OP_ERROR_MODE_IGNORE_ERROR;

    std::vector<uint32_t> attr_counts(object_count);
    std::vector<const sai_attribute_t*> attr_lists(object_count);

    for (uint32_t idx = 0; idx < object_count; idx++)
    {
        attr_counts[idx] = attributes[idx]->get_attr_count();
        attr_lists[idx] = attributes[idx]->get_attr_list();
    }

    switch (objectType)
    {
        case SAI_OBJECT_TYPE_ROUTE_ENTRY:
        {
            std::vector<sai_route_entry_t> entries(object_count);
            for (uint32_t it = 0; it < object_count; it++)
            {
                sai_deserialize_route_entry(objectIds[it], entries[it]);

                entries[it].switch_id = m_translator->translateVidToRid(entries[it].switch_id);
                entries[it].vr_id = m_translator->translateVidToRid(entries[it].vr_id);
            }

            static PerformanceIntervalTimer timer("Syncd::processBulkCreateEntry(route_entry) CREATE");

            timer.start();

            status = m_vendorSai->bulkCreate(
                    object_count,
                    entries.data(),
                    attr_counts.data(),
                    attr_lists.data(),
                    mode,
                    statuses.data());

            timer.stop();

            timer.inc(object_count);
            SWSS_LOG_ERROR("[PERF_TEST] syncd bulk create route objects.");
        }
        break;

        case SAI_OBJECT_TYPE_FDB_ENTRY:
        {
            std::vector<sai_fdb_entry_t> entries(object_count);
            for (uint32_t it = 0; it < object_count; it++)
            {
                sai_deserialize_fdb_entry(objectIds[it], entries[it]);

                entries[it].switch_id = m_translator->translateVidToRid(entries[it].switch_id);
                entries[it].bv_id = m_translator->translateVidToRid(entries[it].bv_id);
            }

            status = m_vendorSai->bulkCreate(
                    object_count,
                    entries.data(),
                    attr_counts.data(),
                    attr_lists.data(),
                    mode,
                    statuses.data());

        }
        break;

        case SAI_OBJECT_TYPE_NAT_ENTRY:
        {
            std::vector<sai_nat_entry_t> entries(object_count);
            for (uint32_t it = 0; it < object_count; it++)
            {
                sai_deserialize_nat_entry(objectIds[it], entries[it]);

                entries[it].switch_id = m_translator->translateVidToRid(entries[it].switch_id);
                entries[it].vr_id = m_translator->translateVidToRid(entries[it].vr_id);
            }

            status = m_vendorSai->bulkCreate(
                    object_count,
                    entries.data(),
                    attr_counts.data(),
                    attr_lists.data(),
                    mode,
                    statuses.data());

        }
        break;

        case SAI_OBJECT_TYPE_INSEG_ENTRY:
        {
            std::vector<sai_inseg_entry_t> entries(object_count);
            for (uint32_t it = 0; it < object_count; it++)
            {
                sai_deserialize_inseg_entry(objectIds[it], entries[it]);

                entries[it].switch_id = m_translator->translateVidToRid(entries[it].switch_id);
         