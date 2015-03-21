import glob
import logging
import os
import Queue
import threading

logger = logging.getLogger(__name__)
class Task:
  """
  This represents a processing task on a set of files
  in a directory. The class creates a pool of configurable size
  and processes all the files in the directory parallely

  A processing task is expected to derive from this class and implement
  process_file and aggregate_results methods
  """
  def __init__(self, dirin, pathout, num_threads=4):
    """
    @param dirin is the directory which has the input files
    @param pathout is the path where the results are stored
    @param num_threads is the size of the workers pool
    """
    self._dirin = dirin
    self._pathout = pathout
    self._results = Queue.Queue()
    self._num_threads = num_threads
    self._inputs = Queue.Queue()

  def run(self):
    """
    Process the files in a thread pool
    """
    for pathin in glob.glob(os.path.join(self._dirin, '*')):
      self._inputs.put(pathin)

    workers = []
    for i in range(self._num_threads):
      w = threading.Thread(target=self._worker_thread, args=(i,))
      w.start()
      workers.append(w)
    for w in workers:
      w.join()

    results = []
    while not self._results.empty():
      results.append(self._results.get())

    self.aggregate_results(results)

  def _worker_thread(self, worker_id):
    while not self._inputs.empty():
      pathin = self._inputs.get()
      self._results.put(self.process_file(pathin))
      logger.info('Processed %s', pathin)
    logger.info('Exiting worker: %s', worker_id)

  def process_file(self, path):
    """
    Process the content of the file and returns the result
    @param path of the file to be processed

    @return an item that would be aggregated by aggregate_results
    """
    assert "Not implemented"

  def aggregate_results(self, results):
    """
    Aggregates results from all the files and dumps it to pathout

    @param results is a list of all the returns from process_file()
       call

    @return None
    """
    assert "Not implemented"
