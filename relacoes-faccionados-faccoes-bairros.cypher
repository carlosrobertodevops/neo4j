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
        faccionadoFaccao:COALESCE(row.faccao,"SEM FACCAO"),
        faccionadoBairroAtual:COALESCE(row.bairro_atual,"SEM BAIRRO"),
        name:COALESCE(row.nome_completo,"SEM NOME")
    }
)

WITH faccionado
LOAD CSV WITH HEADERS FROM "file:///faccao.csv" AS row
MERGE (faccao:Faccao
    {
        faccaoID:COALESCE(row.faccao,"SEM FACCAO"),
        faccaoName:COALESCE(row.faccao,"SEM FACCAO"),
        name:COALESCE(row.faccao,"SEM FACCAO")
    }
)

WITH faccionado, faccao
LOAD CSV WITH HEADERS FROM "file:///bairro.csv" AS row
MERGE (bairro:Bairro
    {
        bairroID:COALESCE(row.bairro_atual,"SEM BAIRRO"),
        bairroName:COALESCE(row.bairro_atual,"SEM BAIRRO"),
        name:COALESCE(row.bairro_atual,"SEM BAIRRO")
    }
)

WITH faccionado, faccao, bairro
MATCH(faccionado{faccionadoFaccao:faccionado.faccionadoFaccao})
MATCH(faccionado{faccionadoBairroAtual:faccionado.faccionadoBairroAtual})
MATCH(faccao{faccaoName:faccao.faccaoName })
MATCH(bairro{bairroName:bairro.bairroName })
WHERE faccao.faccaoName = faccionado.faccionadoFaccao AND bairro.bairroName = faccionado.faccionadoBairroAtual
MERGE (bairro)<-[:`DO BAIRRO`]-(faccionado)-[:`DA FACÇÃO`]->(faccao)

RETURN *