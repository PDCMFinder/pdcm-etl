import luigi


class PdcmConfig(luigi.Config):

    def get_target(self, path):
        return (luigi.LocalTarget(path))


class TimeTaskMixin(object):
    """
    A mixin that will print out the tasks execution time to standard out, when the task is
    finished
    """
    @luigi.Task.event_handler(luigi.Event.PROCESSING_TIME)
    def processing_time(task, processing_time):
        print('Processing time for task {0} was {1} seconds\n'.format(task, processing_time))
