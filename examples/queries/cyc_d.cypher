Match (a:Person)-[e1:knows]->(b:Person)-[e2:knows]->(c:Person), (a)-[e3:knows]->(d:Person)-[e4:knows]->(c) WHERE a.X < 10000000 RETURN MIN(a.X), MIN(b.birthday), MIN(b.X), MIN(c.birthday), MIN(c.X), MIN(d.birthday), MIN(d.X)