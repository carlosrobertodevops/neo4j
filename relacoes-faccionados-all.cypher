// RELACIONAMENTOS DOS FACCIONADOS
// ================================
// faccionados x faccoes (*)
// faccionados x bairros (*)
// fafaccionadosc x cidades (*)

// LOAD CSV WITH HEADERS FROM "file:///faccionados_neo4j_00.csv" AS row
// MERGE(faccionado:Faccionado
//     {
//         faccionadoID:COALESCE(row.index, 0),
//         faccionadoFaccao:COALESCE(row.faccao,"SEM FACCAO"),
//         faccionadoBairro:COALESCE(row.bairro_atual,"SEM BAIRRO"),
//         faccionadoCidade:COALESCE(row.cidade_atual,"SEM CIDADE"),
//         name:COALESCE(row.nome_completo,"SEM NOME")
//     }
// )

// WITH faccionado
// LOAD CSV WITH HEADERS FROM "file:///faccionados_neo4j_00.csv" AS row
// MERGE (faccao:Faccao
//     {
//         faccaoID:COALESCE(row.index, 0),
//         faccaoName:COALESCE(row.faccao,"SEM FACCAO"),
//         name:COALESCE(row.faccao,"SEM FACCAO")
//     }
// )

// WITH faccionado, faccao
// LOAD CSV WITH HEADERS FROM "file:///faccionados_neo4j_00.csv" AS row
// MERGE (bairro:Bairro
//     {
//         bairroID:COALESCE(row.index, 0),
//         bairroName:COALESCE(row.bairro_atual,"SEM BAIRRO"),
//         name:COALESCE(row.bairro_atual,"SEM BAIRRO")
//     }
// )

// WITH faccionado, faccao, bairro
// LOAD CSV WITH HEADERS FROM "file:///faccionados_neo4j_00.csv" AS row
// MERGE (cidade:Cidade
//     {
//         cidadeID:COALESCE(row.index, 0),
//         cidadeName:COALESCE(row.cidade_atual,"SEM CIDADE"),
//         name:COALESCE(row.cidade_atual,"SEM CIDADE")
//     }
// )

// WITH faccionado, faccao, bairro, cidade
// MATCH(faccionado{faccionadoFaccao:faccionado.faccionadoFaccao})
// MATCH(faccionado{faccionadoBairro:faccionado.faccionadoBairro})
// MATCH(faccionado{faccionadoCidade:faccionado.faccionadoCidade})
// MATCH(faccao{faccaoName:faccao.faccaoName})
// MATCH(bairro{bairroName:bairro.bairroName})
// MATCH(cidade{cidadeName:cidade.cidadeName})
// WHERE faccionado.faccionadoFaccao=faccao.faccaoName AND faccionado.faccionadoBairro=bairro.bairroName AND faccionado.faccionadoCidade=cidade.cidadeName
// MERGE (bairro)-[:`DO BAIRRO`]->(faccionado)<-[:`DA FACÇÃO`]-(faccao)
// MERGE (faccionado)-[:`DA CIDADE`]->(cidade)
// RETURN *


// RELACIONAMENTOS DOS FACCIONADOS
// ================================
// faccionados x faccoes
// faccionados x funcao
// faccionados x bairros
// faccionados x cidades

LOAD CSV WITH HEADERS FROM "file:///faccionados_neo4j_00.csv" AS row
MERGE(faccionado:Faccionado
    {
        faccionadoFaccao:COALESCE(row.faccao,"SEM FACCAO"),
        faccionadoFuncao:COALESCE(row.funcao,"SEM FUNCAO"),
        faccionadoBairro:COALESCE(row.bairro_atual,"SEM BAIRRO"),
        faccionadoCidade:COALESCE(row.cidade_atual,"SEM CIDADE"),
        name:COALESCE(row.nome_completo,"SEM NOME")
    }
)

WITH faccionado
LOAD CSV WITH HEADERS FROM "file:///faccionados_neo4j_00.csv" AS row
MERGE (faccao:Faccao
    {
        faccaoName:COALESCE(row.faccao,"SEM FACCAO"),
        name:COALESCE(row.faccao,"SEM FACCAO")
    }
)

WITH faccionado, faccao
LOAD CSV WITH HEADERS FROM "file:///faccionados_neo4j_00.csv" AS row
MERGE (funcao:Funcao
    {
        funcaoName:COALESCE(row.funcao,"SEM FUNCAO"),
        name:COALESCE(row.funcao,"SEM FUNCAO")
    }
)

WITH faccionado, faccao, funcao
LOAD CSV WITH HEADERS FROM "file:///faccionados_neo4j_00.csv" AS row
MERGE (bairro:Bairro
    {
        bairroName:COALESCE(row.bairro_atual,"SEM BAIRRO"),
        name:COALESCE(row.bairro_atual,"SEM BAIRRO")
    }
)

WITH faccionado, faccao, funcao, bairro
LOAD CSV WITH HEADERS FROM "file:///faccionados_neo4j_00.csv" AS row
MERGE (cidade:Cidade
    {
        cidadeName:COALESCE(row.cidade_atual,"SEM CIDADE"),
        name:COALESCE(row.cidade_atual,"SEM CIDADE")
    }
)

WITH faccionado, faccao, funcao, bairro, cidade

MATCH(faccionado{faccionadoFaccao:faccionado.faccionadoFaccao})
MATCH(faccionado{faccionadoFuncao:faccionado.faccionadoFuncao})
MATCH(faccionado{faccionadoBairro:faccionado.faccionadoBairro})
MATCH(faccionado{faccionadoCidade:faccionado.faccionadoCidade})

MATCH(faccao{faccaoName:faccao.faccaoName })
MATCH(funcao{funcaoName:faccao.funcaoName })
MATCH(bairro{bairroName:bairro.bairroName })
MATCH(cidade{cidadeName:cidade.cidadeName })

WHERE faccao.faccaoName = faccionado.faccionadoFaccao AND funcao.funcaoName = faccionado.faccionadoFuncao AND bairro.bairroName = faccionado.faccionadoBairro AND cidade.cidadeName = faccionado.faccionadoCidade 

MERGE (bairro)-[:`DO BAIRRO`]->(faccionado)<-[:`DA FACÇÃO`]-(faccao)
MERGE (cidade)-[:`DA CIDADE`]->(faccionado)<-[:`FUNCAO`]-(funcao)

RETURN *