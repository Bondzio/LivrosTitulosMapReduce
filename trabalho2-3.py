##################################################
# BigData - Arq.Sistemas Distribuídos 2019       #
# Trabalho prático 1 - Map Reduce                #
# Vide Pág.: 65 para as instruções do trabalho   #
# Aluno: Otávio Augusto de Queiroz Reis          #
##################################################

import mincemeat
import glob
import json

#CONSTANTES
RESULT_FILE = 'RESULT_2.3.txt'

files = glob.glob('./files/*')

def file_contents(file_name):
    f = open(file_name)
    try:
        return f.read()
    finally:
        f.close()

source = dict((file_name, file_contents(file_name))for file_name in files)

def mapfn(filename, lines):
    import re #REGEX
    print 'map ' + filename
    from stopwords import allStopWords
    for line in lines.splitlines():
        line_splited = line.split(":::")
        authors = line_splited[1].split('::')
        words = line_splited[2].lower()
        #remove todos os caracteres que não sejam espaço e alfanumérico
        words_normalized = re.sub('[^a-z0-9 ]+','',words).split(" ")
        for author in authors:
            for word in words_normalized:
                if (word not in allStopWords):
                    yield (author, word)

def reducefn(author, word):
    print 'reduce ' + author
    import collections
    dict_ = collections.Counter(word)
    #ordena as palavras de acordo com o count
    result = sorted(dict_.iteritems(), key=lambda item: -item[1])
    return result

s = mincemeat.Server()

s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="changeme")

filter = ["Grzegorz Rozenberg", "Philip S. Yu"]

f = open(RESULT_FILE,'w')
for author in sorted(results.iterkeys()):
    if(filter.count > 0 and author in filter):
        f.write(str(author) + " -> ")
        #apenas as duas palavras que mais aparecem
        for word in results[author][:2]:
            word_count = word[0] + ":" + str(word[1]) + "  "
            f.write(word_count)
        f.write("\n")

#REMOVIDO POR SER MUITO LENTO
# ordered_json = json.dumps(results, sort_keys=True, indent=4, separators=(',', ': '))
# f = open(RESULT_FILE,'w')
# f.write(ordered_json)