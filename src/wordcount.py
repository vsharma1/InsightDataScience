import logging

import task
import tokenizer

logger = logging.getLogger(__name__)

class WordCounter(task.Task):
  """
  This class counts words and aggregates them alphabetically
  """
  def process_file(self, pathin):
    with open(pathin, 'r') as fin:
      words = {}
      # Since we dont process the text line by line, we would treat
      # new line as a space
      for t in tokenizer.WordTokenizer(fin, treat_newline_as_space=True):
        if t.type == tokenizer.Token.kWord:
          # Lower case the word
          text = t.text.lower()
          words[text] = words.get(text, 0) + 1

      return words

  def aggregate_results(self, results):
    words = {}
    # Aggregate the results over all the files
    for r in results:
      for k in r:
        words[k] = words.get(k, 0) + r[k]

    with open(self._pathout, 'w') as fout:
      # Sort the result alphabetically
      keys = words.keys()
      keys.sort()

      for k in keys:
        # Minor formatting to the result
        print >>fout, '%-20s' % k, words[k]
