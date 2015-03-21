import bisect
import logging

import task
import tokenizer

logger = logging.getLogger(__name__)

class MedianRunner(task.Task):
  """
  Calculates the running median of a group of text files
  """
  def process_file(self, pathin):
    with open(pathin, 'r') as fin:
      total_words = []
      numwords_in_line = 0
      for t in tokenizer.WordTokenizer(fin, treat_newline_as_space=False):
        if t.type == tokenizer.Token.kWord:
          # If not end of line, add to the number of words in this line
          numwords_in_line += 1
        elif t.type == tokenizer.Token.kNewLine:
          # On end of line, append to the total number of words/line
          total_words.append(numwords_in_line)
          numwords_in_line = 0

      # Handle the case with no eol at eof
      if numwords_in_line > 0:
        total_words.append(numwords_in_line)
      return (pathin, total_words)

  def aggregate_results(self, results):
    # Sort the result alphabetically by the filename
    results.sort()
    totals = []
    for _,word_count in results:
      # Put the wordcount/line all together
      totals.extend(word_count)

    running_totals = []
    with open(self._pathout, 'w') as fout:
      for t in totals:
        # Calculate the running median
        m = self._new_median(running_totals, t)
        print >> fout, m

  def _new_median(self, totals, num):
    # Insert into the sorted array
    idx = bisect.bisect_left(totals, num)
    totals.insert(idx, num)

    len_medians = len(totals)
    m = 0
    if len_medians % 2 == 0:
      m = totals[(len_medians + 1)/2] + totals[(len_medians - 1)/2]
      m = m / 2.0
    else:
      m = totals[len_medians/2]
    logger.debug('Totals: %s, median: %s', totals, m)
    return round(m, 1)
