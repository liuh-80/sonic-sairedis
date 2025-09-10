#include "swss/logger.h"
#include <signal.h>

int syncd_main(int argc, char **argv);


void sigwinch_handler(int signo)
{
    exit(0);
}

int main(int argc, char **argv)
{
    SWSS_LOG_ENTER();

    if (signal(SIGWINCH, sigwinch_handler) == SIG_ERR)
    {
        SWSS_LOG_ERROR("failed to setup SIGWINCH  action");
        exit(1);
    }

    return syncd_main(argc, argv);
}
