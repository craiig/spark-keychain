val test = sc.textFile("/reviews_books_first_100000.json")
test.persist
test.count
//test.filter(line => line.contains("awesome")).count()
