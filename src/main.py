import logging

import medianrunner
import wordcount

logging.basicConfig(level=logging.INFO)

t = wordcount.WordCounter('wc_input', 'wc_output/wc_result.txt')
t.run()

t = medianrunner.MedianRunner('wc_input', 'wc_output/med_result.txt')
t.run()
