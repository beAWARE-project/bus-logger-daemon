from bus_consumer import BusConsumer


class BusLogger:
    def __init__(self, listen_to_topics):
        self.topics = listen_to_topics

        # Create consumer object
        self.consumer = BusConsumer()

    def run(self):
        print("Initiated listener on topics", self.topics)

        # Start listening to bus
        self.consumer.listen(self.topics)

    def empty_database(self):
        self.consumer.empty_sqlite_database()
        self.consumer.empty_mysql_database()


if __name__ == "__main__":
    topics = [
        'TOP021_INCIDENT_REPORT',
        'TOP101_INCIDENT_REPORT',
        'TOP017_video_analyzed',
        'TOP018_image_analyzed',
        'TOP028_TEXT_ANALYSED',
        'TOP030_REPORT_REQUESTED',
        'TOP040_TEXT_REPORT_GENERATED',
        'TOP040_text_report_generated_demo',
        'social_media_text_demo',
		'TOP010_AUDIO_ANALYZED',
		'TOP001_social_media_text',
		'TOP104_METRIC_REPORT'
    ]

    # Decorate terminal
    print('\033[95m' + "\n***********************")
    print("*** BUS LOGGER v1.0 ***")
    print("***********************\n" + '\033[0m')

    # Create logger
    logger = BusLogger(topics)

    # Prompt to empty database
    # empty_db = input('\033[1m' + "Delete older messages from DB? (y/n)\n" + '\033[0m')
    # if (empty_db == 'Y') or (empty_db == 'y'):
    #     logger.empty_database()

    # Start logger
    logger.run()

