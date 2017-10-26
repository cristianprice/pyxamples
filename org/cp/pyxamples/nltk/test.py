from nltk.tag import DefaultTagger

tagger = DefaultTagger('NN')
result = tagger.tag(['Hello', 'World', 'want'])
print(result)
