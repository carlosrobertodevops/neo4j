// RELACIONAMENTOS DOS FACCIONADOS
// ================================
// faccionados x faccoes (*)
// faccionados x bairros (*)
// fafaccionadosc x cidades (*)
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
        faccionadoBairroAtual:COALESCE(row.bairro_atual,"SEM BAIRRO"),
        faccionadoCidadeAtual:COALESCE(row.cidade_atual,"SEM CIDADE"),
        name:COALESCE(row.nome_completo,"SEM NOME")
    }
)

WITH faccionado
LOAD CSV WITH HEADERS FROM "file:///faccionados_neo4j_00.csv" AS row
MERGE (faccao:Faccao
    {
        faccaoID:COALESCE(row.faccao,"SEM FACCAO"),
        faccaoName:COALESCE(row.faccao,"SEM FACCAO"),
        name:COALESCE(row.faccao,"SEM FACCAO")
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
LOAD CSV WITH HEADERS FROM "file:///faccionados_neo4j_00.csv" AS row
MERGE (cidade:Cidade
    {
        cidadeID:COALESCE(row.cidade_atual,"SEM CIDADE"),
        cidadeName:COALESCE(row.cidade_atual,"SEM CIDADE"),
        name:COALESCE(row.cidade_atual,"SEM CIDADE")
    }
)

WITH faccionado,faccao,bairro,cidade
MATCH(order{faccaoNome:order.faccaoNome})
MATCH(faccionado{faccionadoFaccao:faccionado.faccionadoFaccao})
MATCH(faccionado{faccionadoBairroAtual:faccionado.faccionadoBairroAtual})
MATCH(faccao{faccaoName:faccao.faccaoName })
MATCH(bairro{bairroName:bairro.bairroName })
MATCH(cidade{cidadeName:cidade.cidadeName })

WHERE faccao.faccaoName = faccionado.faccionadoFaccao
    AND bairro.bairroName = faccionado.faccionadoBairroAtual
    AND cidade.cidadeName = faccionado.faccionadoCidadeAtual
MERGE (bairro)<-[:`DO BAIRRO`]-(faccionado)-[:`DA FACÇÃO`]->(faccao)
MERGE (cidade)<-[:`DA CIDADE`]-(faccionado)

RETURN *