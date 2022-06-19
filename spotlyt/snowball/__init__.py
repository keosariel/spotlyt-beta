from __future__ import absolute_import

from io import StringIO

from xapian import SimpleStopper
from . import english, spanish, french, german, italian, russian

def build_stopwords(language, encoding="utf8"):
    file = StringIO(language.stopwords)
    stopwords = []
    for line in file.readlines():
        word = str(line.strip().split("|")[0].strip()).encode(encoding)
        if word:
            stopwords.append(word.decode("utf8"))
    return stopwords

stopwords = {
    "english" : build_stopwords(english, encoding="utf-8"),
    "spanish" : build_stopwords(spanish, encoding="utf-8"),
    "russian" : build_stopwords(russian, encoding="ISO-8859-1"),
    "french" : build_stopwords(french, encoding="utf-8"),
    "german" : build_stopwords(german, encoding="ISO-8859-1"),
    "italian" : build_stopwords(italian, encoding="ISO-8859-1"),
}

stoppers = {}

for code in stopwords:
    stopper = SimpleStopper()
    for word in stopwords[code]:
        stopper.add(word)
    stoppers[code] = stopper





