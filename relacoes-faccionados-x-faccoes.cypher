// Faccionados x faccoes
LOAD CSV WITH HEADERS FROM "file:///faccionados_neo4j_00.csv" AS row
MERGE(faccionados:Faccionados
        {
            faccionadoID: COALESCE(row.index, 0),
            faccionadoName:COALESCE(row.nome_completo,"SEM NOME"),
            faccaoName:COALESCE(row.faccao,"SEM NOME"),
            name:COALESCE(row.nome_completo,"SEM NOME")
        }
    )

WITH faccionados
LOAD CSV WITH HEADERS FROM "file:///faccionados_neo4j_00.csv" AS row
MERGE (faccoes:Faccoes
        {
            faccoesID:COALESCE(row.faccao,"SEM NOME"),
            faccaoName:COALESCE(row.faccao,"SEM NOME"),
            name:COALESCE(row.faccao,"SEM NOME")
        }
    )

WITH faccionados,faccoes
MATCH(faccionados{faccionadoID:faccionados.faccionadoID})
MATCH(faccoes{faccoesID:faccoes.faccoesID })
WHERE faccionados.faccaoName = faccoes.faccaoName
MERGE (faccionados)-[:`FACCIONADO NO`]->(faccoes)

RETURN *
