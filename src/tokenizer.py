"""
This module is responsible for tokenizing a stream of text into
words efficiently.

Usage:
w = WordTokenizer(open(..), treat_newline_as_space=False)
for t in w:
  print t
"""
import logging

logger = logging.getLogger(__name__)

class Token:
  """
  A token is emitted from the WordTokenizer when it sees a token
  There are two or three types of token (depending on the configuration
  of WordTokenizer). They are:
    kWord: Contigous block of alpha numerics
    kNonAlNum: Contigous block of non alpha numerics
    kNewLine: \n character
  """
  kWord = 0
  kNonAlNum = 1
  kNewLine = 2

  def __init__(self, type, text):
    """
    @param type of the token (kWord, kNonAlNum, kNewLine)
    @param text of the token
    """
    self._type = type
    self._text = text

  @property
  def type(self):
    return self._type

  @property
  def text(self):
    return self._text

  def _type_str(self, t):
    if t == Token.kWord:
      return 'kWord'
    elif t == Token.kNonAlNum:
      return 'kNonAlNum'
    else:
      return 'kNewLine'

  def __str__(self):
    return '<%s, %s>' % (self._type_str(self._type), self._text)

class WordTokenizer(object):
  """
  An efficient word tokenizer that tokenizes a stream of text and
  emits a stream of tokens. It reads the stream in blocks of configurable
  size. It can treat newline char as part of space token (for applications
  that dont care for lines).

  The logic removes hyphens and ' to get a word, so hi-lite is treated as hilite
  and we're is treated as were

  The class is expected to be used as:
  for t in WordTokenizer(..):
    consume(t)
  """
  def __init__(self, stream, treat_newline_as_space, read_size=4096):
    """
    @param stream which is processed (expects to have read() function)
    @param treat_newline_as_space is set to True if the application doest care
      for newlines
    @param read_size to read stream (defauls to 4096)
    """
    self._stream = stream
    self._treat_newline_as_space = treat_newline_as_space
    self._read_size = read_size

  def __iter__(self):
    kStateBegin = 0
    kStateInAWord = 1
    kStateInNonAlNum = 2
    kStateAtNewLine = 3

    state = kStateBegin
    remainder = ''
    while True:
      data = self._stream.read(self._read_size)
      # Remove '-' and ' from the text
      data = data.replace('-', '').replace("'", '')
      if len(data) == 0: break

      start = 0
      curr = -1
      for c in data:
        logger.debug('c:%s', c)
        curr += 1

        if state == kStateBegin:
          if c == '\n':
            state = kStateAtNewLine
          if c.isalnum():
            state = kStateInAWord
          else:
            state = kStateInNonAlNum
          start = curr

        elif state == kStateInNonAlNum:
          if c.isalnum():
            t = remainder + self._text(data, start, curr)
            logger.debug('Token: kNonAlNum txt: "%s"', t)
            yield Token(Token.kNonAlNum, t)
            remainder = ''
            state = kStateInAWord
            start = curr
          elif c == '\n' and not self._treat_newline_as_space:
            t = remainder + self._text(data, start, curr)
            logger.debug('Token: kNonAlNum txt: "%s"', t)
            yield Token(Token.kNonAlNum, t)
            remainder = ''
            state = kStateAtNewLine
            start = curr

        elif state == kStateAtNewLine:
          if not self._treat_newline_as_space:
            logger.debug('Token: kNewLine')
            yield Token(Token.kNewLine, '\n')

          if c == '\n':
            state = kStateAtNewLine
          if c.isalnum():
            state = kStateInAWord
          else:
            state = kStateInNonAlNum
          start = curr

        elif state == kStateInAWord:
          if c.isalnum(): continue

          t = remainder + self._text(data, start, curr)
          logger.debug('Token: kWord txt: "%s"', t)
          yield Token(Token.kWord, t)
          remainder = ''

          if c == '\n':
            state = kStateAtNewLine
          elif not c.isalnum():
            state = kStateInNonAlNum
          start = curr

      remainder += data[start:curr + 1]

  def _text(self, data, start, curr):
    return data[start:curr]
