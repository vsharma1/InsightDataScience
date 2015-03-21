import glob
import logging
import os
import Queue
import threading

logger = logging.getLogger(__name__)
class Task:
  def __init__(self, dirin, pathout):
    self._dirin = dirin
    self._pathout = pathout
    self._results = Queue.Queue()

  def run(self):
    workers = []
    for pathin in glob.glob(os.path.join(self._dirin, '*')):
      w = threading.Thread(target=self._worker_thread, args=(pathin,))
      w.start()
      workers.append(w)
    for w in workers:
      w.join()

    results = []
    while not self._results.empty():
      results.append(self._results.get())

    self.aggregate_results(results)

  def _worker_thread(self, pathin):
    self._results.put(self.process_file(pathin))
    logger.info('Processed %s', pathin)

  def process_file(self, pathin):
    assert "Not implemented"

  def aggregate_results(self, results):
    assert "Not implemented"
