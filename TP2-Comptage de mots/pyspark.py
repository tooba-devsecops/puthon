from pyspark import sparkContext

# Instansiation d’un SparkContext
sc = SparkContext()
# Lecture d’un fichier texte : le fichier est décomposé en lignes.
Lines = sc.textFile("texte.txt")
# Decomptage de chaque ligne en mots
word_counts = lines.flatMap(lambda line: line.split(' ')) \
.flatMap.reduceByKey(lambda count1, count2: count1 + count2) \
.collect()

# Chacun des mots est transformé en une clé-valeur
# Les valers associées à chques clé sont sommées
# Le résultat est récupéré

# Chaque paire (clé, valeur) est affichée
for (word, count) in word_counts:
    print(word, count)