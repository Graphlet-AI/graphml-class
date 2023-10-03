# This script comes from Open Sanctions and uses their data. The copyright belongs to Open Sanctions, not Graphlet AI.
# This is only here because I fixed some issues I was having issues with their script and fixed them here.
# Find the full script at: https://github.com/opensanctions/offshore-graph#sanctionsoffshores-graph-demo

CREATE CONSTRAINT entity_id IF NOT EXISTS FOR(n:Entity) REQUIRE (n.id) IS UNIQUE;
CREATE CONSTRAINT name_id IF NOT EXISTS FOR(n:name) REQUIRE (n.id) IS UNIQUE;
CREATE CONSTRAINT email_id IF NOT EXISTS FOR(n:email) REQUIRE (n.id) IS UNIQUE;
CREATE CONSTRAINT phone_id IF NOT EXISTS FOR(n:phone) REQUIRE (n.id) IS UNIQUE;
CREATE CONSTRAINT identifier_id IF NOT EXISTS FOR(n:identifier) REQUIRE (n.id) IS UNIQUE;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_name.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:name { id: row.id })
            SET n.caption = row.caption
            
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_identifier.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:identifier { id: row.id })
            SET n.caption = row.caption
            
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_Person.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:Entity { id: row.id })
            SET n.caption = row.caption
            SET n.name = row.name
            SET n.birthDate = row.birthDate
            SET n.nationality = row.nationality
            SET n.bvdId = row.bvdId
            SET n.country = row.country
            SET n.mainCountry = row.mainCountry
            SET n.dunsCode = row.dunsCode
            SET n.deathDate = row.deathDate
            SET n.dissolutionDate = row.dissolutionDate
            SET n.idNumber = row.idNumber
            SET n.innCode = row.innCode
            SET n.incorporationDate = row.incorporationDate
            SET n.jurisdiction = row.jurisdiction
            SET n.leiCode = row.leiCode
            SET n.ogrnCode = row.ogrnCode
            SET n.alias = row.alias
            SET n.passportNumber = row.passportNumber
            SET n.previousName = row.previousName
            SET n.registrationNumber = row.registrationNumber
            SET n.swiftBic = row.swiftBic
            SET n.taxNumber = row.taxNumber
            SET n.vatCode = row.vatCode
            SET n.wikidataId = row.wikidataId
            SET n:Person:LegalEntity
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_LegalEntity.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:Entity { id: row.id })
            SET n.caption = row.caption
            SET n.name = row.name
            SET n.registrationNumber = row.registrationNumber
            SET n.country = row.country
            SET n.legalForm = row.legalForm
            SET n.status = row.status
            SET n.bvdId = row.bvdId
            SET n.mainCountry = row.mainCountry
            SET n.dunsCode = row.dunsCode
            SET n.dissolutionDate = row.dissolutionDate
            SET n.idNumber = row.idNumber
            SET n.innCode = row.innCode
            SET n.incorporationDate = row.incorporationDate
            SET n.jurisdiction = row.jurisdiction
            SET n.leiCode = row.leiCode
            SET n.ogrnCode = row.ogrnCode
            SET n.alias = row.alias
            SET n.previousName = row.previousName
            SET n.swiftBic = row.swiftBic
            SET n.taxNumber = row.taxNumber
            SET n.vatCode = row.vatCode
            SET n.wikidataId = row.wikidataId
            SET n:LegalEntity
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_Oligarch.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:Entity { id: row.id })
            SET n.caption = row.caption
            SET n:Oligarch
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_Sanction.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:Entity { id: row.id })
            SET n.caption = row.caption
            SET n.program = row.program
            SET n.authority = row.authority
            SET n.endDate = row.endDate
            SET n.entity = row.entity
            SET n.startDate = row.startDate
            SET n.authorityId = row.authorityId
            SET n.country = row.country
            SET n.date = row.date
            SET n.listingDate = row.listingDate
            SET n.modifiedAt = row.modifiedAt
            SET n.unscId = row.unscId
            SET n:Sanction
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_DebarredEntity.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:Entity { id: row.id })
            SET n.caption = row.caption
            SET n:DebarredEntity
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_Address.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:Entity { id: row.id })
            SET n.caption = row.caption
            SET n.city = row.city
            SET n.full = row.full
            SET n.country = row.country
            SET n.street = row.street
            SET n.googlePlaceId = row.googlePlaceId
            SET n.name = row.name
            SET n.osmId = row.osmId
            SET n.alias = row.alias
            SET n.previousName = row.previousName
            SET n.wikidataId = row.wikidataId
            SET n:Address
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_Passport.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:Entity { id: row.id })
            SET n.caption = row.caption
            SET n.passportNumber = row.passportNumber
            SET n.number = row.number
            SET n.country = row.country
            SET n.endDate = row.endDate
            SET n.holder = row.holder
            SET n.startDate = row.startDate
            SET n.type = row.type
            SET n.birthDate = row.birthDate
            SET n.date = row.date
            SET n.modifiedAt = row.modifiedAt
            SET n.personalNumber = row.personalNumber
            SET n:Passport:Identification
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_Identification.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:Entity { id: row.id })
            SET n.caption = row.caption
            SET n.number = row.number
            SET n.country = row.country
            SET n.endDate = row.endDate
            SET n.holder = row.holder
            SET n.startDate = row.startDate
            SET n.type = row.type
            SET n.date = row.date
            SET n.modifiedAt = row.modifiedAt
            SET n:Identification
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_SanctionedEntity.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:Entity { id: row.id })
            SET n.caption = row.caption
            SET n:SanctionedEntity
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_Organization.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:Entity { id: row.id })
            SET n.caption = row.caption
            SET n.name = row.name
            SET n.country = row.country
            SET n.legalForm = row.legalForm
            SET n.status = row.status
            SET n.bvdId = row.bvdId
            SET n.mainCountry = row.mainCountry
            SET n.dunsCode = row.dunsCode
            SET n.dissolutionDate = row.dissolutionDate
            SET n.idNumber = row.idNumber
            SET n.innCode = row.innCode
            SET n.incorporationDate = row.incorporationDate
            SET n.jurisdiction = row.jurisdiction
            SET n.leiCode = row.leiCode
            SET n.ogrnCode = row.ogrnCode
            SET n.alias = row.alias
            SET n.previousName = row.previousName
            SET n.registrationNumber = row.registrationNumber
            SET n.swiftBic = row.swiftBic
            SET n.taxNumber = row.taxNumber
            SET n.vatCode = row.vatCode
            SET n.wikidataId = row.wikidataId
            SET n:Organization:LegalEntity
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_Terrorism.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:Entity { id: row.id })
            SET n.caption = row.caption
            SET n:Terrorism
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_Company.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:Entity { id: row.id })
            SET n.caption = row.caption
            SET n.name = row.name
            SET n.incorporationDate = row.incorporationDate
            SET n.jurisdiction = row.jurisdiction
            SET n.registrationNumber = row.registrationNumber
            SET n.bvdId = row.bvdId
            SET n.country = row.country
            SET n.mainCountry = row.mainCountry
            SET n.dunsCode = row.dunsCode
            SET n.dissolutionDate = row.dissolutionDate
            SET n.idNumber = row.idNumber
            SET n.innCode = row.innCode
            SET n.irsCode = row.irsCode
            SET n.jibCode = row.jibCode
            SET n.leiCode = row.leiCode
            SET n.mbsCode = row.mbsCode
            SET n.ogrnCode = row.ogrnCode
            SET n.alias = row.alias
            SET n.pfrNumber = row.pfrNumber
            SET n.previousName = row.previousName
            SET n.cikCode = row.cikCode
            SET n.swiftBic = row.swiftBic
            SET n.taxNumber = row.taxNumber
            SET n.vatCode = row.vatCode
            SET n.voenCode = row.voenCode
            SET n.wikidataId = row.wikidataId
            SET n.ibcRuc = row.ibcRuc
            SET n:Company:Organization:Asset:LegalEntity
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_email.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:email { id: row.id })
            SET n.caption = row.caption
            
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_phone.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:phone { id: row.id })
            SET n.caption = row.caption
            
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_PublicBody.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:Entity { id: row.id })
            SET n.caption = row.caption
            SET n.name = row.name
            SET n.country = row.country
            SET n.legalForm = row.legalForm
            SET n.status = row.status
            SET n.bvdId = row.bvdId
            SET n.mainCountry = row.mainCountry
            SET n.dunsCode = row.dunsCode
            SET n.dissolutionDate = row.dissolutionDate
            SET n.idNumber = row.idNumber
            SET n.innCode = row.innCode
            SET n.incorporationDate = row.incorporationDate
            SET n.jurisdiction = row.jurisdiction
            SET n.leiCode = row.leiCode
            SET n.ogrnCode = row.ogrnCode
            SET n.alias = row.alias
            SET n.previousName = row.previousName
            SET n.registrationNumber = row.registrationNumber
            SET n.swiftBic = row.swiftBic
            SET n.taxNumber = row.taxNumber
            SET n.vatCode = row.vatCode
            SET n.wikidataId = row.wikidataId
            SET n:PublicBody:Organization:LegalEntity
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_Politician.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:Entity { id: row.id })
            SET n.caption = row.caption
            SET n:Politician
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_Crime.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:Entity { id: row.id })
            SET n.caption = row.caption
            SET n:Crime
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_Vessel.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:Entity { id: row.id })
            SET n.caption = row.caption
            SET n.imoNumber = row.imoNumber
            SET n.name = row.name
            SET n.flag = row.flag
            SET n.type = row.type
            SET n.buildDate = row.buildDate
            SET n.crsNumber = row.crsNumber
            SET n.callSign = row.callSign
            SET n.country = row.country
            SET n.nameChangeDate = row.nameChangeDate
            SET n.mmsi = row.mmsi
            SET n.alias = row.alias
            SET n.previousName = row.previousName
            SET n.registrationDate = row.registrationDate
            SET n.registrationNumber = row.registrationNumber
            SET n.wikidataId = row.wikidataId
            SET n:Vessel:Vehicle:Asset
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_Offshore.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:Entity { id: row.id })
            SET n.caption = row.caption
            SET n:Offshore
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_Security.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:Entity { id: row.id })
            SET n.caption = row.caption
            SET n.isin = row.isin
            SET n.name = row.name
            SET n.registrationNumber = row.registrationNumber
            SET n.country = row.country
            SET n.issuer = row.issuer
            SET n.issueDate = row.issueDate
            SET n.maturityDate = row.maturityDate
            SET n.alias = row.alias
            SET n.previousName = row.previousName
            SET n.ticker = row.ticker
            SET n.wikidataId = row.wikidataId
            SET n:Security:Asset
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_BankAccount.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:Entity { id: row.id })
            SET n.caption = row.caption
            SET n.accountNumber = row.accountNumber
            SET n.name = row.name
            SET n.bankName = row.bankName
            SET n.balanceDate = row.balanceDate
            SET n.bic = row.bic
            SET n.country = row.country
            SET n.maxBalanceDate = row.maxBalanceDate
            SET n.alias = row.alias
            SET n.previousName = row.previousName
            SET n.wikidataId = row.wikidataId
            SET n:BankAccount:Asset
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_CryptoWallet.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:Entity { id: row.id })
            SET n.caption = row.caption
            SET n.publicKey = row.publicKey
            SET n.name = row.name
            SET n.currency = row.currency
            SET n.balanceDate = row.balanceDate
            SET n.country = row.country
            SET n.creationDate = row.creationDate
            SET n.alias = row.alias
            SET n.previousName = row.previousName
            SET n.wikidataId = row.wikidataId
            SET n:CryptoWallet
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_StatwnedEnterprise.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:Entity { id: row.id })
            SET n.caption = row.caption
            SET n:StatwnedEnterprise
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_Airplane.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:Entity { id: row.id })
            SET n.caption = row.caption
            SET n.registrationNumber = row.registrationNumber
            SET n.name = row.name
            SET n.country = row.country
            SET n.operator = row.operator
            SET n.owner = row.owner
            SET n.type = row.type
            SET n.buildDate = row.buildDate
            SET n.alias = row.alias
            SET n.previousName = row.previousName
            SET n.registrationDate = row.registrationDate
            SET n.serialNumber = row.serialNumber
            SET n.wikidataId = row.wikidataId
            SET n:Airplane:Vehicle:Asset
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_CloseAssociate.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:Entity { id: row.id })
            SET n.caption = row.caption
            SET n:CloseAssociate
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_FinancialCrime.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:Entity { id: row.id })
            SET n.caption = row.caption
            SET n:FinancialCrime
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_Bank.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:Entity { id: row.id })
            SET n.caption = row.caption
            SET n:Bank
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_Fraud.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:Entity { id: row.id })
            SET n.caption = row.caption
            SET n:Fraud
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_Diplomat.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:Entity { id: row.id })
            SET n.caption = row.caption
            SET n:Diplomat
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_Judge.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:Entity { id: row.id })
            SET n.caption = row.caption
            SET n:Judge
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_WarCrimes.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:Entity { id: row.id })
            SET n.caption = row.caption
            SET n:WarCrimes
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_Spy.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:Entity { id: row.id })
            SET n.caption = row.caption
            SET n:Spy
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_IntergovernmentalOrganization.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:Entity { id: row.id })
            SET n.caption = row.caption
            SET n:IntergovernmentalOrganization
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_CriminalLeadership.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:Entity { id: row.id })
            SET n.caption = row.caption
            SET n:CriminalLeadership
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/node_Theft.csv' AS row
            WITH row WHERE row.id IS NOT NULL
            call { with row
            MERGE (n:Entity { id: row.id })
            SET n.caption = row.caption
            SET n:Theft
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/edge_OWNERSHIP.csv' AS row
            WITH row WHERE row.source_id IS NOT NULL AND row.target_id IS NOT NULL
            call { with row 
            MATCH (s:Entity {id: row.source_id})
            MATCH (t:Entity {id: row.target_id})
            MERGE (s)-[r:OWNERSHIP]->(t)
            SET r.caption = row.caption
            SET r.percentage = row.percentage
            SET r.startDate = row.startDate
            SET r.endDate = row.endDate
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/edge_HAS_NAME.csv' AS row
            WITH row WHERE row.source_id IS NOT NULL AND row.target_id IS NOT NULL
            call { with row 
            MATCH (s:Entity {id: row.source_id})
            MATCH (t:name {id: row.target_id})
            MERGE (s)-[r:HAS_NAME]->(t)
            
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/edge_HAS_IDENTIFIER.csv' AS row
            WITH row WHERE row.source_id IS NOT NULL AND row.target_id IS NOT NULL
            call { with row 
            MATCH (s:Entity {id: row.source_id})
            MATCH (t:identifier {id: row.target_id})
            MERGE (s)-[r:HAS_IDENTIFIER]->(t)
            
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/edge_ENTITY.csv' AS row
            WITH row WHERE row.source_id IS NOT NULL AND row.target_id IS NOT NULL
            call { with row 
            MATCH (s:Entity {id: row.source_id})
            MATCH (t:Entity {id: row.target_id})
            MERGE (s)-[r:ENTITY]->(t)
            
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/edge_ADDRESS_ENTITY.csv' AS row
            WITH row WHERE row.source_id IS NOT NULL AND row.target_id IS NOT NULL
            call { with row 
            MATCH (s:Entity {id: row.source_id})
            MATCH (t:Entity {id: row.target_id})
            MERGE (s)-[r:ADDRESS_ENTITY]->(t)
            
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/edge_HOLDER.csv' AS row
            WITH row WHERE row.source_id IS NOT NULL AND row.target_id IS NOT NULL
            call { with row 
            MATCH (s:Entity {id: row.source_id})
            MATCH (t:Entity {id: row.target_id})
            MERGE (s)-[r:HOLDER]->(t)
            
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/edge_UNKNOWN_LINK.csv' AS row
            WITH row WHERE row.source_id IS NOT NULL AND row.target_id IS NOT NULL
            call { with row 
            MATCH (s:Entity {id: row.source_id})
            MATCH (t:Entity {id: row.target_id})
            MERGE (s)-[r:UNKNOWN_LINK]->(t)
            SET r.caption = row.caption
            SET r.role = row.role
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/edge_FAMILY.csv' AS row
            WITH row WHERE row.source_id IS NOT NULL AND row.target_id IS NOT NULL
            call { with row 
            MATCH (s:Entity {id: row.source_id})
            MATCH (t:Entity {id: row.target_id})
            MERGE (s)-[r:FAMILY]->(t)
            SET r.caption = row.caption
            SET r.relationship = row.relationship
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/edge_DIRECTORSHIP.csv' AS row
            WITH row WHERE row.source_id IS NOT NULL AND row.target_id IS NOT NULL
            call { with row 
            MATCH (s:Entity {id: row.source_id})
            MATCH (t:Entity {id: row.target_id})
            MERGE (s)-[r:DIRECTORSHIP]->(t)
            SET r.caption = row.caption
            SET r.role = row.role
            SET r.startDate = row.startDate
            SET r.endDate = row.endDate
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/edge_HAS_EMAIL.csv' AS row
            WITH row WHERE row.source_id IS NOT NULL AND row.target_id IS NOT NULL
            call { with row 
            MATCH (s:Entity {id: row.source_id})
            MATCH (t:email {id: row.target_id})
            MERGE (s)-[r:HAS_EMAIL]->(t)
            
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/edge_HAS_PHONE.csv' AS row
            WITH row WHERE row.source_id IS NOT NULL AND row.target_id IS NOT NULL
            call { with row 
            MATCH (s:Entity {id: row.source_id})
            MATCH (t:phone {id: row.target_id})
            MERGE (s)-[r:HAS_PHONE]->(t)
            
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/edge_MEMBERSHIP.csv' AS row
            WITH row WHERE row.source_id IS NOT NULL AND row.target_id IS NOT NULL
            call { with row 
            MATCH (s:Entity {id: row.source_id})
            MATCH (t:Entity {id: row.target_id})
            MERGE (s)-[r:MEMBERSHIP]->(t)
            SET r.caption = row.caption
            SET r.role = row.role
            SET r.startDate = row.startDate
            SET r.endDate = row.endDate
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/edge_REPRESENTATION.csv' AS row
            WITH row WHERE row.source_id IS NOT NULL AND row.target_id IS NOT NULL
            call { with row 
            MATCH (s:Entity {id: row.source_id})
            MATCH (t:Entity {id: row.target_id})
            MERGE (s)-[r:REPRESENTATION]->(t)
            SET r.caption = row.caption
            SET r.role = row.role
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/edge_ISSUER.csv' AS row
            WITH row WHERE row.source_id IS NOT NULL AND row.target_id IS NOT NULL
            call { with row 
            MATCH (s:Entity {id: row.source_id})
            MATCH (t:Entity {id: row.target_id})
            MERGE (s)-[r:ISSUER]->(t)
            
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/edge_OWNER.csv' AS row
            WITH row WHERE row.source_id IS NOT NULL AND row.target_id IS NOT NULL
            call { with row 
            MATCH (s:Entity {id: row.source_id})
            MATCH (t:Entity {id: row.target_id})
            MERGE (s)-[r:OWNER]->(t)
            
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/edge_EMPLOYMENT.csv' AS row
            WITH row WHERE row.source_id IS NOT NULL AND row.target_id IS NOT NULL
            call { with row 
            MATCH (s:Entity {id: row.source_id})
            MATCH (t:Entity {id: row.target_id})
            MERGE (s)-[r:EMPLOYMENT]->(t)
            SET r.caption = row.caption
            SET r.role = row.role
            SET r.startDate = row.startDate
            SET r.endDate = row.endDate
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/edge_ASSOCIATE.csv' AS row
            WITH row WHERE row.source_id IS NOT NULL AND row.target_id IS NOT NULL
            call { with row 
            MATCH (s:Entity {id: row.source_id})
            MATCH (t:Entity {id: row.target_id})
            MERGE (s)-[r:ASSOCIATE]->(t)
            SET r.caption = row.caption
            SET r.relationship = row.relationship
            } in transactions of 50000 rows;

            LOAD CSV WITH HEADERS FROM 'https://data.opensanctions.org/contrib/offshore-graph/exports/edge_PARENT.csv' AS row
            WITH row WHERE row.source_id IS NOT NULL AND row.target_id IS NOT NULL
            call { with row 
            MATCH (s:Entity {id: row.source_id})
            MATCH (t:Entity {id: row.target_id})
            MERGE (s)-[r:PARENT]->(t)
            
            } in transactions of 50000 rows;

MATCH (n:name) WITH n, size([p=(n)--() | p]) as size WHERE size <= 1 call { with n DETACH DELETE (n) } in transactions of 50000 rows;
MATCH (n:email) WITH n, size([p=(n)--() | p]) as size WHERE size <= 1 call { with n DETACH DELETE (n) } in transactions of 50000 rows;
MATCH (n:phone) WITH n, size([p=(n)--() | p]) as size WHERE size <= 1 call { with n DETACH DELETE (n) } in transactions of 50000 rows;
MATCH (n:identifier) WITH n, size([p=(n)--() | p]) as size WHERE size <= 1 call { with n DETACH DELETE (n) } in transactions of 50000 rows;
