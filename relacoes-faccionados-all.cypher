// RELACIONAMENTOS DOS FACCIONADOS
// ================================
// faccionados x faccoes
// faccionados x bairros
// fafaccionadosc x cidades
// faccionados x idade
// faccionadosac x homicidas
// faccionadosac x estupradores
// faccionados x assaltantes

LOAD CSV WITH HEADERS FROM "file:///faccionados_neo4j_00.csv" AS row
MERGE(faccionado:Faccionado
    {
        faccionadoID: COALESCE(row.index, 0),
        faccionadoName:COALESCE(row.nome_completo,"SEM NOME"),
        faccionadoFaccao:COALESCE(row.faccao,"SEM NOME"),
        faccionadoBairroAtual:COALESCE(row.bairro_atual,"SEM NOME"),
        name:COALESCE(row.nome_completo,"SEM NOME")
    }
)

WITH faccionado
LOAD CSV WITH HEADERS FROM "file:///faccionados_neo4j_00.csv" AS row
MERGE (faccao:Faccoe
    {
        faccaoID:COALESCE(row.faccao,"SEM NOME"),
        faccaoName:COALESCE(row.faccao,"SEM NOME"),
        name:COALESCE(row.faccao,"SEM NOME")
    }
)

WITH faccionado,faccao
LOAD CSV WITH HEADERS FROM "file:///faccionados_neo4j_00.csv" AS row
MERGE (bairro:Bairro
    {
        bairroID:COALESCE(row.bairro_atual,"SEM BAIRRO"),
        bairroName:COALESCE(row.bairro_atual,"SEM BAIRRO"),
        name:COALESCE(row.bairro_atual,"SEM BAIRRO")
    }
)

WITH faccionado,faccao,bairro
MATCH(faccionado{faccionadoID:faccionado.faccionadoID})
MATCH(faccionado{faccionadoFaccao:faccionado.faccionadoFaccao})
MATCH(faccionado{faccionadoBairroAtual:faccionado.faccionadoID})
MATCH(faccao{faccaoID:faccao.faccaoID })
MATCH(bairro{bairroID:bairro.bairroID })
WHERE faccao.faccaoID = faccionado.faccionadoFaccao AND bairro.bairroID = faccionado.faccionadoBairroAtual
MERGE (bairro)<-[:`DO BAIRRO`]-(faccionado)-[:`DA FACÇÃO`]->(faccao)

RETURN *