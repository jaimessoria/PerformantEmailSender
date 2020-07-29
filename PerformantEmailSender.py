import asyncio
import signal
import argparse
import io
import logging
import sys
import os
import yaml
import logging.config


logger = logging.getLogger(__name__)

class EmailRoutines:
       
    async def email_sender (self, email: int):
        await asyncio.sleep(1) 
        email_state = 'Y' # Y= email sent N = email NO sent
        return email_state

class DbRoutines:

    def email_counter (self, db_path: str):
        logger.info("Reading CSV ")   
        try:    
            num_email =100  # check in DB how many emails there is in the DB 
            logger.info("The number of email to be send:[%i]", num_email)
        except Exception as e:
            logger.error('Reading from file failed')
            logger.error(str(e))
            sys.exit(1)
        return num_email
    
    async def email_fetcher (self, db_path: str, num_email: int):
        if (num_email % 2) == 0:
            await asyncio.sleep(10)   #time to fetch even num from DB 
        else:
            await asyncio.sleep(2)    #time to fetch odd num from DB

        email = "email_"+str(num_email)+"@data.de"
        return email

class PerformantEmailSender:

    def __init__(self, email_routines: EmailRoutines,
                db_routines: DbRoutines):
        self.email_routines = email_routines
        self.db_routines = db_routines

    async def new_email (self, db_path: str, num_email: int):
        try:
            email = await self.db_routines.email_fetcher(db_path, num_email)
            logger.info(
                "Fetcher: num_email: [%i] email: [%s]", num_email, email) 

            email_state = await self.email_routines.email_sender(email)
            logger.info(
                "Sender: email: [%s] sent: [%s] ", email, email_state)

        except Exception as e:
            logger.info( "ERROR in email number: [%i] ", num_email)
            logging.error(str(e))
            sys.exit(1) 
        
def parse_args():
    parser = argparse.ArgumentParser(
        description='Send emails in performant way')

    parser.add_argument('--dbfile', required=True,
                        help='Data Base file name')

    return parser.parse_args()

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
def setup_logging():
    # create logger
    logger.setLevel(logging.DEBUG)
    logging.basicConfig(filename='logs/EmailSender.log',level=logging.DEBUG)

async def main():
    args = parse_args()
=======
=======
>>>>>>> logs
=======
>>>>>>> logs
def setup_logging(
    default_path='logging.yaml',
    default_level=logging.INFO,
    env_key='LOG_CFG'
):
    """Setup logging configuration"""
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


def main():
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> logs
=======
>>>>>>> logs
=======
>>>>>>> logs
    email_routines = EmailRoutines()
    db_routines = DbRoutines()
    performant_email_sender = PerformantEmailSender(email_routines,db_routines)

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
    try:
        num_email = db_routines.email_counter(args.dbfile)
        logger.info("The number of email to be send:[%i]", num_email)

    except Exception as e:
        logger.error('Reading from file failed')
        logger.error(str(e))
        sys.exit(1)

    while num_email > 0:
        await asyncio.create_task(performant_email_sender.new_email(args.dbfile, num_email))
        num_email -= 1
    
=======
    setup_logging()
    loop = asyncio.get_event_loop()
    print('Running. Press CTRL-C to exit.')
    logger.info("Starting application")
    args = parse_args()                                     # take de CSV file path
    num_email = db_routines.email_counter(args.dbfile)      # count de num of emails to send from DB 

    try:
        while num_email > 0:                                #create a new coroutine for each email to send 
            asyncio.ensure_future(performant_email_sender.new_email(args.dbfile, num_email))
            num_email -= 1
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("Received exit signal")
        loop.close()
        sys.exit(1)

>>>>>>> logs
=======
    setup_logging()
    loop = asyncio.get_event_loop()
    print('Running. Press CTRL-C to exit.')
    logger.info("Starting application")
    args = parse_args()                                     # take de CSV file path
    num_email = db_routines.email_counter(args.dbfile)      # count de num of emails to send from DB 

    try:
        while num_email > 0:                                #create a new coroutine for each email to send 
            asyncio.ensure_future(performant_email_sender.new_email(args.dbfile, num_email))
            num_email -= 1
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("Received exit signal")
        loop.close()
        sys.exit(1)

>>>>>>> logs
=======
    setup_logging()
    loop = asyncio.get_event_loop()
    print('Running. Press CTRL-C to exit.')
    logger.info("Starting application")
    args = parse_args()                                     # take de CSV file path
    num_email = db_routines.email_counter(args.dbfile)      # count de num of emails to send from DB 

    try:
        while num_email > 0:                                #create a new coroutine for each email to send 
            asyncio.ensure_future(performant_email_sender.new_email(args.dbfile, num_email))
            num_email -= 1
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("Received exit signal")
        loop.close()
        sys.exit(1)

>>>>>>> logs
    logger.info('All operations completed successfully')
    sys.exit(0)
    
if __name__ == '__main__':
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
    setup_logging()
    logger.info("Starting application")
    loop = asyncio.get_event_loop()
    main_task = asyncio.ensure_future(main())
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(s, main_task.cancel)
    try:
        print('Running. Press CTRL-C to exit.')
        loop.run_until_complete(main_task)
    finally:
        logger.info("Received exit signal")
        loop.close()
        print ("\n Successfully shutdown")
        sys.exit(1)
=======
    main()
>>>>>>> logs
=======
    main()
>>>>>>> logs
=======
    main()
>>>>>>> logs
