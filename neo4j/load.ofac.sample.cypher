// Load the nodes and edges
CALL apoc.import.csv([{fileName: 'file:ofac-1.5-hop-nodes-2.csv', labels: ['Airplane','Asset','Bank','BankAccount','CloseAssociate','Company','Crime','Diplomat','Entity','LegalEntity','Offshore','Oligarch','Organization','Person','Politician','SanctionedEntity','Spy','StatwnedEnterprise','Terrorism','Vehicle','Vessel']}], [{fileName: 'file:ofac-1.5-hop-edges.csv', labels: ['OWNERSHIP','OWNER','DIRECTORSHIP','PARENT']}], {delimiter: ',', arrayDelimiter: ',', stringIds: true});

// LOAD edges
CALL apoc.import.csv([],[{fileName: 'file:ofac-1.5-hop-edges.csv', labels: ['OWNERSHIP','OWNER','DIRECTORSHIP','PARENT']}], {});