
import logging
import logging.config

from carnac.database import Glue2Database

libexec_path = os.path.split(os.path.abspath(__file__))[0]
carnac_path = os.path.split(libexec_path)[0]

logging.config.fileConfig(os.path.join(carnac_path,"etc","logging.conf"))

logger = logging.getLogger("carnac.clean_recent_jobs")

#######################################################################################################################

if __name__ == "__main__":
    db = Glue2Database()
    db.connect()
    try:
        state = self._xmlToQueueState(routing_key,content)
        self._handleQueueState(state,db)
    except Exception:
        traceback.print_exc()
        #logger.error(content)
    db.close()
    
