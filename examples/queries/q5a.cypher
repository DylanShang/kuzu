MATCH (a:Person)-[e1:knows]->(b:Person)-[e2:knows]->(c:Person)-[e3:knows]->(d:Person) WHERE b.X < 10000 RETURN MIN(a.X), MIN(b.X), MIN(c.X), MIN(d.X)