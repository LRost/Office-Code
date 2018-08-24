import pypyodbc 
import pandas as pd

# Connect to SQL database using ODBC
prod_db = pypyodbc.connect("Driver={SQL version};"
                        "Server=Servername;"
                        "Database=Database name;"
                        "uid=ID;pwd=password")

#Complete SQL query into df (DataFrame)
df = pd.read_sql_query("""

SELECT a.fldArtikelcode AS 'Artikelcode'
 ,a.fldOmschrijving AS 'Omschrijving'
 ,CAST (vp.Verkoopprijs AS NUMERIC(36,2))AS 'Verkoopprijs'
 ,CAST (xa.fldAdviesprijsexclBTW AS NUMERIC(36,2))AS 'AdviesExBTW'
 ,CAST (xa.fldMSRP AS NUMERIC(36,2)) AS 'MSRP'
 ,CASE WHEN vrd.fldVrijeVoorraad IS NULL THEN '0' ELSE '1' END AS 'voorraad' 
 ,xa.[fldEAN CODE INVUL] AS 'EAN'
 ,xa.fldArtikelType AS 'Artikeltype'
FROM tblRelatie klant, tblArtikel a
	LEFT JOIN tblArtikelOmzetgroep aog ON aog.fldArtikelOmzetgroepID = a.fldArtikelOmzetgroepID
	LEFT JOIN tblRelatie r ON a.fldLeverancierRelatieID = r.fldRelatieID
	LEFT JOIN tblX_Leverancier xl ON r.fldRelatieID = xl.fldLeverancierRelatieID
	LEFT JOIN tblLand ll ON r.fldLandID = ll.fldLandID
	LEFT JOIN tblArtikelKortingGroep akg ON a.fldArtikelKortingGroepID = akg.fldArtikelKortingGroepID    
	LEFT JOIN (SELECT 0 AS fldID, 'keuze' AS fldNaam UNION SELECT 1, 'nooit' UNION SELECT 2, 'altijd') receptPlaatsen ON receptPlaatsen.fldID = a.fldReceptPlaatsen
	LEFT JOIN tblX_Artikel xa ON xa.fldArtikelID = a.fldArtikelID
	LEFT JOIN (SELECT DISTINCT fldReceptArtikelID, 1 AS HasRecept FROM tblRecept) recept ON a.fldArtikelID = recept.fldReceptArtikelID 
	INNER JOIN
  (
SELECT fldArtikelID, Artikelcode, Omschrijving, ISNULL(Basisprijs,0) AS Basisprijs, P1, P2, P3, P4,
	ISNULL(CASE 
	WHEN ISNULL(Rangorde,'') = '' THEN
		CASE
		WHEN ISNULL(P1,9999999) < ISNULL(P2,9999999) AND ISNULL(P1,9999999) < ISNULL(P3,9999999) AND ISNULL(P1,9999999) < ISNULL(P4,9999999) AND ISNULL(P1,9999999) < ISNULL(Basisprijs,9999999) THEN P1
		WHEN ISNULL(P2,9999999) < ISNULL(P1,9999999) AND ISNULL(P2,9999999) < ISNULL(P3,9999999) AND ISNULL(P2,9999999) < ISNULL(P4,9999999) AND ISNULL(P2,9999999) < ISNULL(Basisprijs,9999999) THEN P2
		WHEN ISNULL(P3,9999999) < ISNULL(P1,9999999) AND ISNULL(P3,9999999) < ISNULL(P2,9999999) AND ISNULL(P3,9999999) < ISNULL(P4,9999999) AND ISNULL(P3,9999999) < ISNULL(Basisprijs,9999999) THEN P3
		WHEN ISNULL(P4,9999999) < ISNULL(P1,9999999) AND ISNULL(P4,9999999) < ISNULL(P2,9999999) AND ISNULL(P4,9999999) < ISNULL(P3,9999999) AND ISNULL(P4,9999999) < ISNULL(Basisprijs,9999999) THEN P4
		ELSE Basisprijs
		END
	ELSE
		CASE
		WHEN Rangorde = 'A' THEN COALESCE(NULL,Basisprijs)
		WHEN Rangorde = 'B' THEN COALESCE(P1,Basisprijs)
		WHEN Rangorde = 'C' THEN COALESCE(P2,Basisprijs)
		WHEN Rangorde = 'D' THEN COALESCE(P3,Basisprijs)
		WHEN Rangorde = 'E' THEN COALESCE(P4,Basisprijs)
		WHEN Rangorde = 'AB' THEN COALESCE(NULL,P1,Basisprijs)
		WHEN Rangorde = 'AC' THEN COALESCE(NULL,P2,Basisprijs)
		WHEN Rangorde = 'AD' THEN COALESCE(NULL,P3,Basisprijs)
		WHEN Rangorde = 'AE' THEN COALESCE(NULL,P4,Basisprijs)
		WHEN Rangorde = 'BA' THEN COALESCE(P1,NULL,Basisprijs)
		WHEN Rangorde = 'BC' THEN COALESCE(P1,P2,Basisprijs)
		WHEN Rangorde = 'BD' THEN COALESCE(P1,P3,Basisprijs)
		WHEN Rangorde = 'BE' THEN COALESCE(P1,P4,Basisprijs)
		WHEN Rangorde = 'CA' THEN COALESCE(P2,NULL,Basisprijs)
		WHEN Rangorde = 'CB' THEN COALESCE(P2,P1,Basisprijs)
		WHEN Rangorde = 'CD' THEN COALESCE(P2,P3,Basisprijs)
		WHEN Rangorde = 'CE' THEN COALESCE(P2,P4,Basisprijs)
		WHEN Rangorde = 'DA' THEN COALESCE(P3,NULL,Basisprijs)
		WHEN Rangorde = 'DB' THEN COALESCE(P3,P1,Basisprijs)
		WHEN Rangorde = 'DC' THEN COALESCE(P3,P2,Basisprijs)
		WHEN Rangorde = 'DE' THEN COALESCE(P3,P4,Basisprijs)
		WHEN Rangorde = 'EA' THEN COALESCE(P4,NULL,Basisprijs)
		WHEN Rangorde = 'EB' THEN COALESCE(P4,P1,Basisprijs)
		WHEN Rangorde = 'EC' THEN COALESCE(P4,P2,Basisprijs)
		WHEN Rangorde = 'ED' THEN COALESCE(P4,P3,Basisprijs)
		WHEN Rangorde = 'ABC' THEN COALESCE(NULL,P1,P2,Basisprijs)
		WHEN Rangorde = 'ABD' THEN COALESCE(NULL,P1,P3,Basisprijs)
		WHEN Rangorde = 'ABE' THEN COALESCE(NULL,P1,P4,Basisprijs)
		WHEN Rangorde = 'ACB' THEN COALESCE(NULL,P2,P1,Basisprijs)
		WHEN Rangorde = 'ACD' THEN COALESCE(NULL,P2,P3,Basisprijs)
		WHEN Rangorde = 'ACE' THEN COALESCE(NULL,P2,P4,Basisprijs)
		WHEN Rangorde = 'ADB' THEN COALESCE(NULL,P3,P1,Basisprijs)
		WHEN Rangorde = 'ADC' THEN COALESCE(NULL,P3,P2,Basisprijs)
		WHEN Rangorde = 'ADE' THEN COALESCE(NULL,P3,P4,Basisprijs)
		WHEN Rangorde = 'AEB' THEN COALESCE(NULL,P4,P1,Basisprijs)
		WHEN Rangorde = 'AEC' THEN COALESCE(NULL,P4,P2,Basisprijs)
		WHEN Rangorde = 'AED' THEN COALESCE(NULL,P4,P3,Basisprijs)
		WHEN Rangorde = 'BAC' THEN COALESCE(P1,NULL,P2,Basisprijs)
		WHEN Rangorde = 'BAD' THEN COALESCE(P1,NULL,P3,Basisprijs)
		WHEN Rangorde = 'BAE' THEN COALESCE(P1,NULL,P4,Basisprijs)
		WHEN Rangorde = 'BCA' THEN COALESCE(P1,P2,NULL,Basisprijs)
		WHEN Rangorde = 'BCD' THEN COALESCE(P1,P2,P3,Basisprijs)
		WHEN Rangorde = 'BCE' THEN COALESCE(P1,P2,P4,Basisprijs)
		WHEN Rangorde = 'BDA' THEN COALESCE(P1,P3,NULL,Basisprijs)
		WHEN Rangorde = 'BDC' THEN COALESCE(P1,P3,P2,Basisprijs)
		WHEN Rangorde = 'BDE' THEN COALESCE(P1,P3,P4,Basisprijs)
		WHEN Rangorde = 'BEA' THEN COALESCE(P1,P4,NULL,Basisprijs)
		WHEN Rangorde = 'BEC' THEN COALESCE(P1,P4,P2,Basisprijs)
		WHEN Rangorde = 'BED' THEN COALESCE(P1,P4,P3,Basisprijs)
		WHEN Rangorde = 'CAB' THEN COALESCE(P2,NULL,P1,Basisprijs)
		WHEN Rangorde = 'CAD' THEN COALESCE(P2,NULL,P3,Basisprijs)
		WHEN Rangorde = 'CAE' THEN COALESCE(P2,NULL,P4,Basisprijs)
		WHEN Rangorde = 'CBA' THEN COALESCE(P2,P1,NULL,Basisprijs)
		WHEN Rangorde = 'CBD' THEN COALESCE(P2,P1,P3,Basisprijs)
		WHEN Rangorde = 'CBE' THEN COALESCE(P2,P1,P4,Basisprijs)
		WHEN Rangorde = 'CDA' THEN COALESCE(P2,P3,NULL,Basisprijs)
		WHEN Rangorde = 'CDB' THEN COALESCE(P2,P3,P1,Basisprijs)
		WHEN Rangorde = 'CDE' THEN COALESCE(P2,P3,P4,Basisprijs)
		WHEN Rangorde = 'CEA' THEN COALESCE(P2,P4,NULL,Basisprijs)
		WHEN Rangorde = 'CEB' THEN COALESCE(P2,P4,P1,Basisprijs)
		WHEN Rangorde = 'CED' THEN COALESCE(P2,P4,P3,Basisprijs)
		WHEN Rangorde = 'DAB' THEN COALESCE(P3,NULL,P1,Basisprijs)
		WHEN Rangorde = 'DAC' THEN COALESCE(P3,NULL,P2,Basisprijs)
		WHEN Rangorde = 'DAE' THEN COALESCE(P3,NULL,P4,Basisprijs)
		WHEN Rangorde = 'DBA' THEN COALESCE(P3,P1,NULL,Basisprijs)
		WHEN Rangorde = 'DBC' THEN COALESCE(P3,P1,P2,Basisprijs)
		WHEN Rangorde = 'DBE' THEN COALESCE(P3,P1,P4,Basisprijs)
		WHEN Rangorde = 'DCA' THEN COALESCE(P3,P2,NULL,Basisprijs)
		WHEN Rangorde = 'DCB' THEN COALESCE(P3,P2,P1,Basisprijs)
		WHEN Rangorde = 'DCE' THEN COALESCE(P3,P2,P4,Basisprijs)
		WHEN Rangorde = 'DEA' THEN COALESCE(P3,P4,NULL,Basisprijs)
		WHEN Rangorde = 'DEB' THEN COALESCE(P3,P4,P1,Basisprijs)
		WHEN Rangorde = 'DEC' THEN COALESCE(P3,P4,P2,Basisprijs)
		WHEN Rangorde = 'EAB' THEN COALESCE(P4,NULL,P1,Basisprijs)
		WHEN Rangorde = 'EAC' THEN COALESCE(P4,NULL,P2,Basisprijs)
		WHEN Rangorde = 'EAD' THEN COALESCE(P4,NULL,P3,Basisprijs)
		WHEN Rangorde = 'EBA' THEN COALESCE(P4,P1,NULL,Basisprijs)
		WHEN Rangorde = 'EBC' THEN COALESCE(P4,P1,P2,Basisprijs)
		WHEN Rangorde = 'EBD' THEN COALESCE(P4,P1,P3,Basisprijs)
		WHEN Rangorde = 'ECA' THEN COALESCE(P4,P2,NULL,Basisprijs)
		WHEN Rangorde = 'ECB' THEN COALESCE(P4,P2,P1,Basisprijs)
		WHEN Rangorde = 'ECD' THEN COALESCE(P4,P2,P3,Basisprijs)
		WHEN Rangorde = 'EDA' THEN COALESCE(P4,P3,NULL,Basisprijs)
		WHEN Rangorde = 'EDB' THEN COALESCE(P4,P3,P1,Basisprijs)
		WHEN Rangorde = 'EDC' THEN COALESCE(P4,P3,P2,Basisprijs)
		WHEN Rangorde = 'ABCD' THEN COALESCE(NULL,P1,P2,P3,Basisprijs)
		WHEN Rangorde = 'ABCE' THEN COALESCE(NULL,P1,P2,P4,Basisprijs)
		WHEN Rangorde = 'ABDC' THEN COALESCE(NULL,P1,P3,P2,Basisprijs)
		WHEN Rangorde = 'ABDE' THEN COALESCE(NULL,P1,P3,P4,Basisprijs)
		WHEN Rangorde = 'ABEC' THEN COALESCE(NULL,P1,P4,P2,Basisprijs)
		WHEN Rangorde = 'ABED' THEN COALESCE(NULL,P1,P4,P3,Basisprijs)
		WHEN Rangorde = 'ACBD' THEN COALESCE(NULL,P2,P1,P3,Basisprijs)
		WHEN Rangorde = 'ACBE' THEN COALESCE(NULL,P2,P1,P4,Basisprijs)
		WHEN Rangorde = 'ACDB' THEN COALESCE(NULL,P2,P3,P1,Basisprijs)
		WHEN Rangorde = 'ACDE' THEN COALESCE(NULL,P2,P3,P4,Basisprijs)
		WHEN Rangorde = 'ACEB' THEN COALESCE(NULL,P2,P4,P1,Basisprijs)
		WHEN Rangorde = 'ACED' THEN COALESCE(NULL,P2,P4,P3,Basisprijs)
		WHEN Rangorde = 'ADBC' THEN COALESCE(NULL,P3,P1,P2,Basisprijs)
		WHEN Rangorde = 'ADBE' THEN COALESCE(NULL,P3,P1,P4,Basisprijs)
		WHEN Rangorde = 'ADCB' THEN COALESCE(NULL,P3,P2,P1,Basisprijs)
		WHEN Rangorde = 'ADCE' THEN COALESCE(NULL,P3,P2,P4,Basisprijs)
		WHEN Rangorde = 'ADEB' THEN COALESCE(NULL,P3,P4,P1,Basisprijs)
		WHEN Rangorde = 'ADEC' THEN COALESCE(NULL,P3,P4,P2,Basisprijs)
		WHEN Rangorde = 'AEBC' THEN COALESCE(NULL,P4,P1,P2,Basisprijs)
		WHEN Rangorde = 'AEBD' THEN COALESCE(NULL,P4,P1,P3,Basisprijs)
		WHEN Rangorde = 'AECB' THEN COALESCE(NULL,P4,P2,P1,Basisprijs)
		WHEN Rangorde = 'AECD' THEN COALESCE(NULL,P4,P2,P3,Basisprijs)
		WHEN Rangorde = 'AEDB' THEN COALESCE(NULL,P4,P3,P1,Basisprijs)
		WHEN Rangorde = 'AEDC' THEN COALESCE(NULL,P4,P3,P2,Basisprijs)
		WHEN Rangorde = 'BACD' THEN COALESCE(P1,NULL,P2,P3,Basisprijs)
		WHEN Rangorde = 'BACE' THEN COALESCE(P1,NULL,P2,P4,Basisprijs)
		WHEN Rangorde = 'BADC' THEN COALESCE(P1,NULL,P3,P2,Basisprijs)
		WHEN Rangorde = 'BADE' THEN COALESCE(P1,NULL,P3,P4,Basisprijs)
		WHEN Rangorde = 'BAEC' THEN COALESCE(P1,NULL,P4,P2,Basisprijs)
		WHEN Rangorde = 'BAED' THEN COALESCE(P1,NULL,P4,P3,Basisprijs)
		WHEN Rangorde = 'BCAD' THEN COALESCE(P1,P2,NULL,P3,Basisprijs)
		WHEN Rangorde = 'BCAE' THEN COALESCE(P1,P2,NULL,P4,Basisprijs)
		WHEN Rangorde = 'BCDA' THEN COALESCE(P1,P2,P3,NULL,Basisprijs)
		WHEN Rangorde = 'BCDE' THEN COALESCE(P1,P2,P3,P4,Basisprijs)
		WHEN Rangorde = 'BCEA' THEN COALESCE(P1,P2,P4,NULL,Basisprijs)
		WHEN Rangorde = 'BCED' THEN COALESCE(P1,P2,P4,P3,Basisprijs)
		WHEN Rangorde = 'BDAC' THEN COALESCE(P1,P3,NULL,P2,Basisprijs)
		WHEN Rangorde = 'BDAE' THEN COALESCE(P1,P3,NULL,P4,Basisprijs)
		WHEN Rangorde = 'BDCA' THEN COALESCE(P1,P3,P2,NULL,Basisprijs)
		WHEN Rangorde = 'BDCE' THEN COALESCE(P1,P3,P2,P4,Basisprijs)
		WHEN Rangorde = 'BDEA' THEN COALESCE(P1,P3,P4,NULL,Basisprijs)
		WHEN Rangorde = 'BDEC' THEN COALESCE(P1,P3,P4,P2,Basisprijs)
		WHEN Rangorde = 'BEAC' THEN COALESCE(P1,P4,NULL,P2,Basisprijs)
		WHEN Rangorde = 'BEAD' THEN COALESCE(P1,P4,NULL,P3,Basisprijs)
		WHEN Rangorde = 'BECA' THEN COALESCE(P1,P4,P2,NULL,Basisprijs)
		WHEN Rangorde = 'BECD' THEN COALESCE(P1,P4,P2,P3,Basisprijs)
		WHEN Rangorde = 'BEDA' THEN COALESCE(P1,P4,P3,NULL,Basisprijs)
		WHEN Rangorde = 'BEDC' THEN COALESCE(P1,P4,P3,P2,Basisprijs)
		WHEN Rangorde = 'CABD' THEN COALESCE(P2,NULL,P1,P3,Basisprijs)
		WHEN Rangorde = 'CABE' THEN COALESCE(P2,NULL,P1,P4,Basisprijs)
		WHEN Rangorde = 'CADB' THEN COALESCE(P2,NULL,P3,P1,Basisprijs)
		WHEN Rangorde = 'CADE' THEN COALESCE(P2,NULL,P3,P4,Basisprijs)
		WHEN Rangorde = 'CAEB' THEN COALESCE(P2,NULL,P4,P1,Basisprijs)
		WHEN Rangorde = 'CAED' THEN COALESCE(P2,NULL,P4,P3,Basisprijs)
		WHEN Rangorde = 'CBAD' THEN COALESCE(P2,P1,NULL,P3,Basisprijs)
		WHEN Rangorde = 'CBAE' THEN COALESCE(P2,P1,NULL,P4,Basisprijs)
		WHEN Rangorde = 'CBDA' THEN COALESCE(P2,P1,P3,NULL,Basisprijs)
		WHEN Rangorde = 'CBDE' THEN COALESCE(P2,P1,P3,P4,Basisprijs)
		WHEN Rangorde = 'CBEA' THEN COALESCE(P2,P1,P4,NULL,Basisprijs)
		WHEN Rangorde = 'CBED' THEN COALESCE(P2,P1,P4,P3,Basisprijs)
		WHEN Rangorde = 'CDAB' THEN COALESCE(P2,P3,NULL,P1,Basisprijs)
		WHEN Rangorde = 'CDAE' THEN COALESCE(P2,P3,NULL,P4,Basisprijs)
		WHEN Rangorde = 'CDBA' THEN COALESCE(P2,P3,P1,NULL,Basisprijs)
		WHEN Rangorde = 'CDBE' THEN COALESCE(P2,P3,P1,P4,Basisprijs)
		WHEN Rangorde = 'CDEA' THEN COALESCE(P2,P3,P4,NULL,Basisprijs)
		WHEN Rangorde = 'CDEB' THEN COALESCE(P2,P3,P4,P1,Basisprijs)
		WHEN Rangorde = 'CEAB' THEN COALESCE(P2,P4,NULL,P1,Basisprijs)
		WHEN Rangorde = 'CEAD' THEN COALESCE(P2,P4,NULL,P3,Basisprijs)
		WHEN Rangorde = 'CEBA' THEN COALESCE(P2,P4,P1,NULL,Basisprijs)
		WHEN Rangorde = 'CEBD' THEN COALESCE(P2,P4,P1,P3,Basisprijs)
		WHEN Rangorde = 'CEDA' THEN COALESCE(P2,P4,P3,NULL,Basisprijs)
		WHEN Rangorde = 'CEDB' THEN COALESCE(P2,P4,P3,P1,Basisprijs)
		WHEN Rangorde = 'DABC' THEN COALESCE(P3,NULL,P1,P2,Basisprijs)
		WHEN Rangorde = 'DABE' THEN COALESCE(P3,NULL,P1,P4,Basisprijs)
		WHEN Rangorde = 'DACB' THEN COALESCE(P3,NULL,P2,P1,Basisprijs)
		WHEN Rangorde = 'DACE' THEN COALESCE(P3,NULL,P2,P4,Basisprijs)
		WHEN Rangorde = 'DAEB' THEN COALESCE(P3,NULL,P4,P1,Basisprijs)
		WHEN Rangorde = 'DAEC' THEN COALESCE(P3,NULL,P4,P2,Basisprijs)
		WHEN Rangorde = 'DBAC' THEN COALESCE(P3,P1,NULL,P2,Basisprijs)
		WHEN Rangorde = 'DBAE' THEN COALESCE(P3,P1,NULL,P4,Basisprijs)
		WHEN Rangorde = 'DBCA' THEN COALESCE(P3,P1,P2,NULL,Basisprijs)
		WHEN Rangorde = 'DBCE' THEN COALESCE(P3,P1,P2,P4,Basisprijs)
		WHEN Rangorde = 'DBEA' THEN COALESCE(P3,P1,P4,NULL,Basisprijs)
		WHEN Rangorde = 'DBEC' THEN COALESCE(P3,P1,P4,P2,Basisprijs)
		WHEN Rangorde = 'DCAB' THEN COALESCE(P3,P2,NULL,P1,Basisprijs)
		WHEN Rangorde = 'DCAE' THEN COALESCE(P3,P2,NULL,P4,Basisprijs)
		WHEN Rangorde = 'DCBA' THEN COALESCE(P3,P2,P1,NULL,Basisprijs)
		WHEN Rangorde = 'DCBE' THEN COALESCE(P3,P2,P1,P4,Basisprijs)
		WHEN Rangorde = 'DCEA' THEN COALESCE(P3,P2,P4,NULL,Basisprijs)
		WHEN Rangorde = 'DCEB' THEN COALESCE(P3,P2,P4,P1,Basisprijs)
		WHEN Rangorde = 'DEAB' THEN COALESCE(P3,P4,NULL,P1,Basisprijs)
		WHEN Rangorde = 'DEAC' THEN COALESCE(P3,P4,NULL,P2,Basisprijs)
		WHEN Rangorde = 'DEBA' THEN COALESCE(P3,P4,P1,NULL,Basisprijs)
		WHEN Rangorde = 'DEBC' THEN COALESCE(P3,P4,P1,P2,Basisprijs)
		WHEN Rangorde = 'DECA' THEN COALESCE(P3,P4,P2,NULL,Basisprijs)
		WHEN Rangorde = 'DECB' THEN COALESCE(P3,P4,P2,P1,Basisprijs)
		WHEN Rangorde = 'EABC' THEN COALESCE(P4,NULL,P1,P2,Basisprijs)
		WHEN Rangorde = 'EABD' THEN COALESCE(P4,NULL,P1,P3,Basisprijs)
		WHEN Rangorde = 'EACB' THEN COALESCE(P4,NULL,P2,P1,Basisprijs)
		WHEN Rangorde = 'EACD' THEN COALESCE(P4,NULL,P2,P3,Basisprijs)
		WHEN Rangorde = 'EADB' THEN COALESCE(P4,NULL,P3,P1,Basisprijs)
		WHEN Rangorde = 'EADC' THEN COALESCE(P4,NULL,P3,P2,Basisprijs)
		WHEN Rangorde = 'EBAC' THEN COALESCE(P4,P1,NULL,P2,Basisprijs)
		WHEN Rangorde = 'EBAD' THEN COALESCE(P4,P1,NULL,P3,Basisprijs)
		WHEN Rangorde = 'EBCA' THEN COALESCE(P4,P1,P2,NULL,Basisprijs)
		WHEN Rangorde = 'EBCD' THEN COALESCE(P4,P1,P2,P3,Basisprijs)
		WHEN Rangorde = 'EBDA' THEN COALESCE(P4,P1,P3,NULL,Basisprijs)
		WHEN Rangorde = 'EBDC' THEN COALESCE(P4,P1,P3,P2,Basisprijs)
		WHEN Rangorde = 'ECAB' THEN COALESCE(P4,P2,NULL,P1,Basisprijs)
		WHEN Rangorde = 'ECAD' THEN COALESCE(P4,P2,NULL,P3,Basisprijs)
		WHEN Rangorde = 'ECBA' THEN COALESCE(P4,P2,P1,NULL,Basisprijs)
		WHEN Rangorde = 'ECBD' THEN COALESCE(P4,P2,P1,P3,Basisprijs)
		WHEN Rangorde = 'ECDA' THEN COALESCE(P4,P2,P3,NULL,Basisprijs)
		WHEN Rangorde = 'ECDB' THEN COALESCE(P4,P2,P3,P1,Basisprijs)
		WHEN Rangorde = 'EDAB' THEN COALESCE(P4,P3,NULL,P1,Basisprijs)
		WHEN Rangorde = 'EDAC' THEN COALESCE(P4,P3,NULL,P2,Basisprijs)
		WHEN Rangorde = 'EDBA' THEN COALESCE(P4,P3,P1,NULL,Basisprijs)
		WHEN Rangorde = 'EDBC' THEN COALESCE(P4,P3,P1,P2,Basisprijs)
		WHEN Rangorde = 'EDCA' THEN COALESCE(P4,P3,P2,NULL,Basisprijs)
		WHEN Rangorde = 'EDCB' THEN COALESCE(P4,P3,P2,P1,Basisprijs)
		WHEN Rangorde = 'ABCDE' THEN COALESCE(NULL,P1,P2,P3,P4,Basisprijs)
		WHEN Rangorde = 'ABCED' THEN COALESCE(NULL,P1,P2,P4,P3,Basisprijs)
		WHEN Rangorde = 'ABDCE' THEN COALESCE(NULL,P1,P3,P2,P4,Basisprijs)
		WHEN Rangorde = 'ABDEC' THEN COALESCE(NULL,P1,P3,P4,P2,Basisprijs)
		WHEN Rangorde = 'ABECD' THEN COALESCE(NULL,P1,P4,P2,P3,Basisprijs)
		WHEN Rangorde = 'ABEDC' THEN COALESCE(NULL,P1,P4,P3,P2,Basisprijs)
		WHEN Rangorde = 'ACBDE' THEN COALESCE(NULL,P2,P1,P3,P4,Basisprijs)
		WHEN Rangorde = 'ACBED' THEN COALESCE(NULL,P2,P1,P4,P3,Basisprijs)
		WHEN Rangorde = 'ACDBE' THEN COALESCE(NULL,P2,P3,P1,P4,Basisprijs)
		WHEN Rangorde = 'ACDEB' THEN COALESCE(NULL,P2,P3,P4,P1,Basisprijs)
		WHEN Rangorde = 'ACEBD' THEN COALESCE(NULL,P2,P4,P1,P3,Basisprijs)
		WHEN Rangorde = 'ACEDB' THEN COALESCE(NULL,P2,P4,P3,P1,Basisprijs)
		WHEN Rangorde = 'ADBCE' THEN COALESCE(NULL,P3,P1,P2,P4,Basisprijs)
		WHEN Rangorde = 'ADBEC' THEN COALESCE(NULL,P3,P1,P4,P2,Basisprijs)
		WHEN Rangorde = 'ADCBE' THEN COALESCE(NULL,P3,P2,P1,P4,Basisprijs)
		WHEN Rangorde = 'ADCEB' THEN COALESCE(NULL,P3,P2,P4,P1,Basisprijs)
		WHEN Rangorde = 'ADEBC' THEN COALESCE(NULL,P3,P4,P1,P2,Basisprijs)
		WHEN Rangorde = 'ADECB' THEN COALESCE(NULL,P3,P4,P2,P1,Basisprijs)
		WHEN Rangorde = 'AEBCD' THEN COALESCE(NULL,P4,P1,P2,P3,Basisprijs)
		WHEN Rangorde = 'AEBDC' THEN COALESCE(NULL,P4,P1,P3,P2,Basisprijs)
		WHEN Rangorde = 'AECBD' THEN COALESCE(NULL,P4,P2,P1,P3,Basisprijs)
		WHEN Rangorde = 'AECDB' THEN COALESCE(NULL,P4,P2,P3,P1,Basisprijs)
		WHEN Rangorde = 'AEDBC' THEN COALESCE(NULL,P4,P3,P1,P2,Basisprijs)
		WHEN Rangorde = 'AEDCB' THEN COALESCE(NULL,P4,P3,P2,P1,Basisprijs)
		WHEN Rangorde = 'BACDE' THEN COALESCE(P1,NULL,P2,P3,P4,Basisprijs)
		WHEN Rangorde = 'BACED' THEN COALESCE(P1,NULL,P2,P4,P3,Basisprijs)
		WHEN Rangorde = 'BADCE' THEN COALESCE(P1,NULL,P3,P2,P4,Basisprijs)
		WHEN Rangorde = 'BADEC' THEN COALESCE(P1,NULL,P3,P4,P2,Basisprijs)
		WHEN Rangorde = 'BAECD' THEN COALESCE(P1,NULL,P4,P2,P3,Basisprijs)
		WHEN Rangorde = 'BAEDC' THEN COALESCE(P1,NULL,P4,P3,P2,Basisprijs)
		WHEN Rangorde = 'BCADE' THEN COALESCE(P1,P2,NULL,P3,P4,Basisprijs)
		WHEN Rangorde = 'BCAED' THEN COALESCE(P1,P2,NULL,P4,P3,Basisprijs)
		WHEN Rangorde = 'BCDAE' THEN COALESCE(P1,P2,P3,NULL,P4,Basisprijs)
		WHEN Rangorde = 'BCDEA' THEN COALESCE(P1,P2,P3,P4,NULL,Basisprijs)
		WHEN Rangorde = 'BCEAD' THEN COALESCE(P1,P2,P4,NULL,P3,Basisprijs)
		WHEN Rangorde = 'BCEDA' THEN COALESCE(P1,P2,P4,P3,NULL,Basisprijs)
		WHEN Rangorde = 'BDACE' THEN COALESCE(P1,P3,NULL,P2,P4,Basisprijs)
		WHEN Rangorde = 'BDAEC' THEN COALESCE(P1,P3,NULL,P4,P2,Basisprijs)
		WHEN Rangorde = 'BDCAE' THEN COALESCE(P1,P3,P2,NULL,P4,Basisprijs)
		WHEN Rangorde = 'BDCEA' THEN COALESCE(P1,P3,P2,P4,NULL,Basisprijs)
		WHEN Rangorde = 'BDEAC' THEN COALESCE(P1,P3,P4,NULL,P2,Basisprijs)
		WHEN Rangorde = 'BDECA' THEN COALESCE(P1,P3,P4,P2,NULL,Basisprijs)
		WHEN Rangorde = 'BEACD' THEN COALESCE(P1,P4,NULL,P2,P3,Basisprijs)
		WHEN Rangorde = 'BEADC' THEN COALESCE(P1,P4,NULL,P3,P2,Basisprijs)
		WHEN Rangorde = 'BECAD' THEN COALESCE(P1,P4,P2,NULL,P3,Basisprijs)
		WHEN Rangorde = 'BECDA' THEN COALESCE(P1,P4,P2,P3,NULL,Basisprijs)
		WHEN Rangorde = 'BEDAC' THEN COALESCE(P1,P4,P3,NULL,P2,Basisprijs)
		WHEN Rangorde = 'BEDCA' THEN COALESCE(P1,P4,P3,P2,NULL,Basisprijs)
		WHEN Rangorde = 'CABDE' THEN COALESCE(P2,NULL,P1,P3,P4,Basisprijs)
		WHEN Rangorde = 'CABED' THEN COALESCE(P2,NULL,P1,P4,P3,Basisprijs)
		WHEN Rangorde = 'CADBE' THEN COALESCE(P2,NULL,P3,P1,P4,Basisprijs)
		WHEN Rangorde = 'CADEB' THEN COALESCE(P2,NULL,P3,P4,P1,Basisprijs)
		WHEN Rangorde = 'CAEBD' THEN COALESCE(P2,NULL,P4,P1,P3,Basisprijs)
		WHEN Rangorde = 'CAEDB' THEN COALESCE(P2,NULL,P4,P3,P1,Basisprijs)
		WHEN Rangorde = 'CBADE' THEN COALESCE(P2,P1,NULL,P3,P4,Basisprijs)
		WHEN Rangorde = 'CBAED' THEN COALESCE(P2,P1,NULL,P4,P3,Basisprijs)
		WHEN Rangorde = 'CBDAE' THEN COALESCE(P2,P1,P3,NULL,P4,Basisprijs)
		WHEN Rangorde = 'CBDEA' THEN COALESCE(P2,P1,P3,P4,NULL,Basisprijs)
		WHEN Rangorde = 'CBEAD' THEN COALESCE(P2,P1,P4,NULL,P3,Basisprijs)
		WHEN Rangorde = 'CBEDA' THEN COALESCE(P2,P1,P4,P3,NULL,Basisprijs)
		WHEN Rangorde = 'CDABE' THEN COALESCE(P2,P3,NULL,P1,P4,Basisprijs)
		WHEN Rangorde = 'CDAEB' THEN COALESCE(P2,P3,NULL,P4,P1,Basisprijs)
		WHEN Rangorde = 'CDBAE' THEN COALESCE(P2,P3,P1,NULL,P4,Basisprijs)
		WHEN Rangorde = 'CDBEA' THEN COALESCE(P2,P3,P1,P4,NULL,Basisprijs)
		WHEN Rangorde = 'CDEAB' THEN COALESCE(P2,P3,P4,NULL,P1,Basisprijs)
		WHEN Rangorde = 'CDEBA' THEN COALESCE(P2,P3,P4,P1,NULL,Basisprijs)
		WHEN Rangorde = 'CEABD' THEN COALESCE(P2,P4,NULL,P1,P3,Basisprijs)
		WHEN Rangorde = 'CEADB' THEN COALESCE(P2,P4,NULL,P3,P1,Basisprijs)
		WHEN Rangorde = 'CEBAD' THEN COALESCE(P2,P4,P1,NULL,P3,Basisprijs)
		WHEN Rangorde = 'CEBDA' THEN COALESCE(P2,P4,P1,P3,NULL,Basisprijs)
		WHEN Rangorde = 'CEDAB' THEN COALESCE(P2,P4,P3,NULL,P1,Basisprijs)
		WHEN Rangorde = 'CEDBA' THEN COALESCE(P2,P4,P3,P1,NULL,Basisprijs)
		WHEN Rangorde = 'DABCE' THEN COALESCE(P3,NULL,P1,P2,P4,Basisprijs)
		WHEN Rangorde = 'DABEC' THEN COALESCE(P3,NULL,P1,P4,P2,Basisprijs)
		WHEN Rangorde = 'DACBE' THEN COALESCE(P3,NULL,P2,P1,P4,Basisprijs)
		WHEN Rangorde = 'DACEB' THEN COALESCE(P3,NULL,P2,P4,P1,Basisprijs)
		WHEN Rangorde = 'DAEBC' THEN COALESCE(P3,NULL,P4,P1,P2,Basisprijs)
		WHEN Rangorde = 'DAECB' THEN COALESCE(P3,NULL,P4,P2,P1,Basisprijs)
		WHEN Rangorde = 'DBACE' THEN COALESCE(P3,P1,NULL,P2,P4,Basisprijs)
		WHEN Rangorde = 'DBAEC' THEN COALESCE(P3,P1,NULL,P4,P2,Basisprijs)
		WHEN Rangorde = 'DBCAE' THEN COALESCE(P3,P1,P2,NULL,P4,Basisprijs)
		WHEN Rangorde = 'DBCEA' THEN COALESCE(P3,P1,P2,P4,NULL,Basisprijs)
		WHEN Rangorde = 'DBEAC' THEN COALESCE(P3,P1,P4,NULL,P2,Basisprijs)
		WHEN Rangorde = 'DBECA' THEN COALESCE(P3,P1,P4,P2,NULL,Basisprijs)
		WHEN Rangorde = 'DCABE' THEN COALESCE(P3,P2,NULL,P1,P4,Basisprijs)
		WHEN Rangorde = 'DCAEB' THEN COALESCE(P3,P2,NULL,P4,P1,Basisprijs)
		WHEN Rangorde = 'DCBAE' THEN COALESCE(P3,P2,P1,NULL,P4,Basisprijs)
		WHEN Rangorde = 'DCBEA' THEN COALESCE(P3,P2,P1,P4,NULL,Basisprijs)
		WHEN Rangorde = 'DCEAB' THEN COALESCE(P3,P2,P4,NULL,P1,Basisprijs)
		WHEN Rangorde = 'DCEBA' THEN COALESCE(P3,P2,P4,P1,NULL,Basisprijs)
		WHEN Rangorde = 'DEABC' THEN COALESCE(P3,P4,NULL,P1,P2,Basisprijs)
		WHEN Rangorde = 'DEACB' THEN COALESCE(P3,P4,NULL,P2,P1,Basisprijs)
		WHEN Rangorde = 'DEBAC' THEN COALESCE(P3,P4,P1,NULL,P2,Basisprijs)
		WHEN Rangorde = 'DEBCA' THEN COALESCE(P3,P4,P1,P2,NULL,Basisprijs)
		WHEN Rangorde = 'DECAB' THEN COALESCE(P3,P4,P2,NULL,P1,Basisprijs)
		WHEN Rangorde = 'DECBA' THEN COALESCE(P3,P4,P2,P1,NULL,Basisprijs)
		WHEN Rangorde = 'EABCD' THEN COALESCE(P4,NULL,P1,P2,P3,Basisprijs)
		WHEN Rangorde = 'EABDC' THEN COALESCE(P4,NULL,P1,P3,P2,Basisprijs)
		WHEN Rangorde = 'EACBD' THEN COALESCE(P4,NULL,P2,P1,P3,Basisprijs)
		WHEN Rangorde = 'EACDB' THEN COALESCE(P4,NULL,P2,P3,P1,Basisprijs)
		WHEN Rangorde = 'EADBC' THEN COALESCE(P4,NULL,P3,P1,P2,Basisprijs)
		WHEN Rangorde = 'EADCB' THEN COALESCE(P4,NULL,P3,P2,P1,Basisprijs)
		WHEN Rangorde = 'EBACD' THEN COALESCE(P4,P1,NULL,P2,P3,Basisprijs)
		WHEN Rangorde = 'EBADC' THEN COALESCE(P4,P1,NULL,P3,P2,Basisprijs)
		WHEN Rangorde = 'EBCAD' THEN COALESCE(P4,P1,P2,NULL,P3,Basisprijs)
		WHEN Rangorde = 'EBCDA' THEN COALESCE(P4,P1,P2,P3,NULL,Basisprijs)
		WHEN Rangorde = 'EBDAC' THEN COALESCE(P4,P1,P3,NULL,P2,Basisprijs)
		WHEN Rangorde = 'EBDCA' THEN COALESCE(P4,P1,P3,P2,NULL,Basisprijs)
		WHEN Rangorde = 'ECABD' THEN COALESCE(P4,P2,NULL,P1,P3,Basisprijs)
		WHEN Rangorde = 'ECADB' THEN COALESCE(P4,P2,NULL,P3,P1,Basisprijs)
		WHEN Rangorde = 'ECBAD' THEN COALESCE(P4,P2,P1,NULL,P3,Basisprijs)
		WHEN Rangorde = 'ECBDA' THEN COALESCE(P4,P2,P1,P3,NULL,Basisprijs)
		WHEN Rangorde = 'ECDAB' THEN COALESCE(P4,P2,P3,NULL,P1,Basisprijs)
		WHEN Rangorde = 'ECDBA' THEN COALESCE(P4,P2,P3,P1,NULL,Basisprijs)
		WHEN Rangorde = 'EDABC' THEN COALESCE(P4,P3,NULL,P1,P2,Basisprijs)
		WHEN Rangorde = 'EDACB' THEN COALESCE(P4,P3,NULL,P2,P1,Basisprijs)
		WHEN Rangorde = 'EDBAC' THEN COALESCE(P4,P3,P1,NULL,P2,Basisprijs)
		WHEN Rangorde = 'EDBCA' THEN COALESCE(P4,P3,P1,P2,NULL,Basisprijs)
		WHEN Rangorde = 'EDCAB' THEN COALESCE(P4,P3,P2,NULL,P1,Basisprijs)
		WHEN Rangorde = 'EDCBA' THEN COALESCE(P4,P3,P2,P1,NULL,Basisprijs)
		END
	END, 0) AS Verkoopprijs
  FROM (SELECT artikel.fldArtikelID, artikel.fldArtikelcode AS Artikelcode, artikel.fldOmschrijving AS Omschrijving, artikel.fldVerkoopprijs AS Basisprijs
  , Rangorde
	, CASE WHEN inst.fldPrijsafspraakArtikelPerKlant<>0 THEN ISNULL(ArtikelKlant.vp, artikel.fldVerkoopprijs * (100 - ArtikelKlant.krt) / 100) 
      END AS P1
	, CASE WHEN inst.fldPrijsafspraakArtikelKortinggroepPerKlant<>0 THEN artikel.fldVerkoopprijs * (100 - ArtikelgroepKlant.krt) / 100 
      END AS P2
	, CASE WHEN inst.fldPrijsafspraakArtikelPerKlantKortinggroep<>0 THEN ISNULL(ArtikelKlantgroep.vp, artikel.fldVerkoopprijs * (100 - ArtikelKlantgroep.krt) / 100) 
      END AS P3
	, CASE WHEN inst.fldPrijsafspraakArtikelKortinggroepPerKlantKortinggroep<>0 THEN artikel.fldVerkoopprijs * (100 - ArtikelgroepKlantgroep.krt) / 100 
      END AS P4
     FROM tblInstelling inst, 
		(SELECT fldPrijsafspraakRangorde AS Rangorde FROM tblInstelling) instelling
    , qryArtikel artikel 
	LEFT JOIN 
    (SELECT k.fldArtikelID
          , k.fldVerkoopprijs AS vp
          , k.fldKorting AS krt 
      FROM tblPrijsAfspraakArtikelKlant k 
      WHERE k.fldVanafAantal=0 AND k.fldRelatieID='1803') ArtikelKlant ON artikel.fldArtikelID = ArtikelKlant.fldArtikelID 
	LEFT JOIN 
    (SELECT k.fldArtikelID
    , k.fldVerkoopprijs AS vp
    , k.fldKorting AS krt 
      FROM tblPrijsAfspraakArtikelKlantKortinggroep k 
      WHERE k.fldVanafAantal=0 AND k.fldKlantKortinggroepID = 
          (SELECT fldKlantKortinggroepID FROM tblRelatie WHERE fldRelatieID = '1803')
      ) ArtikelKlantgroep ON artikel.fldArtikelID = ArtikelKlantgroep.fldArtikelID 
	LEFT JOIN 
    (SELECT k.fldArtikelKortinggroepID, NULL AS vp
          , k.fldKorting AS krt 
      FROM tblPrijsAfspraakArtikelKortinggroepKlant k 
      WHERE k.fldVanafAantal=0 AND k.fldRelatieID='1803') 
        ArtikelgroepKlant ON artikel.fldArtikelKortinggroepID = ArtikelgroepKlant.fldArtikelKortinggroepID 
	LEFT JOIN 
    (SELECT k.fldArtikelKortinggroepID, NULL AS vp
          , k.fldKorting AS krt 
       FROM tblPrijsAfspraakArtikelKortinggroepKlantKortinggroep k 
       WHERE k.fldVanafAantal=0 AND k.fldKlantKortinggroepID = 
                (SELECT fldKlantKortinggroepID FROM tblRelatie WHERE fldRelatieID = '1803'))
           ArtikelgroepKlantgroep ON artikel.fldArtikelKortinggroepID = ArtikelgroepKlantgroep.fldArtikelKortinggroepID 
     ) Prijsafspraken
  ) vp ON vp.fldArtikelID = a.fldArtikelID LEFT JOIN
  (SELECT DISTINCT fldArtikelID FROM tblVerkoopOrderRegel 
      INNER JOIN tblVerkoopOrder ON tblVerkoopOrderRegel.fldVerkoopOrderID=tblVerkoopOrder.fldVerkoopOrderID 
          WHERE tblVerkoopOrderRegel.fldArtikelID IS NOT NULL 
          AND (tblVerkoopOrder.fldRelatieID = '1803' OR tblVerkoopOrder.fldVerzendRelatieID = '1803')) 
      AS hist ON a.fldArtikelID = hist.fldArtikelID
 LEFT JOIN (SELECT mutaties.fldArtikelID, SUM(mutaties.fldVrijeVoorraad) AS fldVrijeVoorraad
				FROM (
					SELECT 
						'InkoopOrderRegelID_' + CAST(iore.fldInkoopOrderregelID AS VARCHAR) AS RegelID
            , iore.fldArtikelID
            , iore.fldAantal
            , iore.fldOmschrijving AS MutatieOmschrijving
            , avmr.fldOmschrijving AS VoorraadmutatieReden
            , CASE WHEN avmr.fldVoorraadAf = 1 
                AND avmr.fldArtikelVoorraadMutatieRedenID <> 10 
                  THEN 'Af' ELSE 'Bij' END 
                AS VoorraadBijAf, 
						CASE WHEN inst.fldVoorraadBeginDatum <= COALESCE(iord.fldDatumOntvangen, iord.fldDatumJournaalpost) AND iord.fldOntvangst <> 0 THEN
							CASE WHEN avmr.fldVoorraadAf = 1 AND avmr.fldArtikelVoorraadMutatieRedenID <> 10 THEN -1 ELSE 1 END * iore.fldAantal
						END AS fldTechnischeVoorraad,
						  CASE WHEN inst.fldVoorraadBeginDatum <= COALESCE(iord.fldDatumOntvangen, iord.fldDatumJournaalpost) AND iord.fldOntvangst <> 0 THEN
							CASE WHEN avmr.fldVoorraadAf = 1 AND avmr.fldArtikelVoorraadMutatieRedenID <> 10 THEN -1 ELSE 1 END * iore.fldAantal 
						END AS fldVrijeVoorraad,
						  CASE WHEN iord.fldOrdernummer <> 0 THEN COALESCE(iord.fldDatumOntvangen, iord.fldDatumBesteld) END AS fldInkoopdatum,
						NULL AS fldVerkoopdatum
					FROM dbo.tblInstelling inst,
						dbo.tblInkoopOrderRegel iore INNER JOIN
						dbo.tblInkoopOrder iord ON iore.fldInkoopOrderID = iord.fldInkoopOrderID LEFT JOIN 
						dbo.tblArtikelVoorraadMutatieReden avmr ON iord.fldArtikelVoorraadMutatieRedenID = avmr.fldArtikelVoorraadMutatieRedenID
					WHERE 
						iore.fldArtikelID IS NOT null
					UNION
					SELECT ALL
						'VerkoopOrderRegelID_' + CAST(vor.fldVerkoopOrderRegelID AS VARCHAR) AS RegelID, vor.fldArtikelID
            , vor.fldAantalLeveren
            , vor.fldOmschrijving AS MutatieOmschrijving,
						avmr.fldOmschrijving AS VoorraadmutatieReden
            , CASE WHEN avmr.fldVoorraadAf = 1 
            OR avmr.fldArtikelVoorraadMutatieRedenID = 0 
            OR avmr.fldArtikelVoorraadMutatieRedenID = 11 
            THEN 'Af' ELSE 'Bij' END AS VoorraadBijAf,
						CASE WHEN inst.fldVoorraadBeginDatum <= CASE WHEN inst.fldVerkooporderVoorraadTellingNiveau = 60 
                   THEN COALESCE(vf.fldDatum,cb.fldDatumTijd) ELSE vo.fldDatum END
							AND (CASE WHEN COALESCE(vf.flddatum,cb.fldDatumTijd) IS NOT NULL THEN 1 ELSE 0 END
								 + CASE WHEN inst.fldVerkooporderVoorraadTellingNiveau <= 50 AND (vo.fldPakbonAfgedrukt = 1 OR vo.fldAfhaalbonAfgedrukt = 1) THEN 1 ELSE 0 END
								 + CASE WHEN inst.fldVerkooporderVoorraadTellingNiveau <= 40 AND vo.fldWerkbonAfgedrukt = 1 THEN 1 ELSE 0 END
								 + CASE WHEN inst.fldVerkooporderVoorraadTellingNiveau <= 30 AND vo.fldBevestigingAfgedrukt = 1 THEN 1 ELSE 0 END
								 + CASE WHEN inst.fldVerkooporderVoorraadTellingNiveau <= 25 AND vo.fldBackorderVolgnummer IS NOT NULL THEN 1 ELSE 0 END
								) >= 1 THEN
							CASE WHEN avmr.fldVoorraadAf = 1 
                OR avmr.fldArtikelVoorraadMutatieRedenID = 0 
                OR avmr.fldArtikelVoorraadMutatieRedenID = 11 
              THEN -1 ELSE 1 END * vor.fldAantalLeveren 
						END AS fldTechnischeVoorraad,
						CASE WHEN inst.fldVoorraadBeginDatum <= 
              CASE WHEN inst.fldVerkooporderVoorraadTellingNiveau = 60
              THEN COALESCE(vf.fldDatum,cb.fldDatumTijd) ELSE vo.fldDatum END
							AND (CASE WHEN COALESCE(vf.flddatum,cb.fldDatumTijd) IS NOT NULL THEN 1 ELSE 0 END
								 + CASE WHEN inst.fldVerkooporderVoorraadTellingNiveau <= 50 AND (vo.fldPakbonAfgedrukt = 1 
                      OR vo.fldAfhaalbonAfgedrukt = 1) THEN 1 ELSE 0 END
								 + CASE WHEN inst.fldVerkooporderVoorraadTellingNiveau <= 40 AND vo.fldWerkbonAfgedrukt = 1 THEN 1 ELSE 0 END
								 + CASE WHEN inst.fldVerkooporderVoorraadTellingNiveau <= 30 AND vo.fldBevestigingAfgedrukt = 1 THEN 1 ELSE 0 END
								 + CASE WHEN inst.fldVerkooporderVoorraadTellingNiveau <= 25 AND vo.fldBackorderVolgnummer IS NOT NULL THEN 1 ELSE 0 END
								) >= 1 THEN
							CASE WHEN avmr.fldVoorraadAf = 1 
                OR avmr.fldArtikelVoorraadMutatieRedenID = 0 
                OR avmr.fldArtikelVoorraadMutatieRedenID = 11 THEN -1 ELSE 1 END * vor.fldAantalLeveren
						ELSE 0 END 
						+ CASE WHEN inst.fldVoorraadBeginDatum <= vo.fldDatum THEN -1 * vor.fldAantalReserveren ELSE 0 END AS fldVrijeVoorraad,
						NULL AS fldInkoopdatum,
						CASE WHEN vo.fldOrdernummer <> 0 THEN vo.fldDatum END AS fldVerkoopdatum
					FROM dbo.tblInstelling inst,
						dbo.tblVerkoopOrderRegel vor 
            INNER JOIN dbo.tblVerkoopOrder vo ON vor.fldVerkoopOrderID = vo.fldVerkoopOrderID 
            LEFT JOIN dbo.tblArtikelVoorraadMutatieReden avmr ON vo.fldArtikelVoorraadMutatieRedenID = avmr.fldArtikelVoorraadMutatieRedenID 
            LEFT JOIN dbo.tblVerkoopFactuur vf ON vo.fldVerkoopFactuurID = vf.fldVerkoopFactuurID 
            LEFT JOIN dbo.tblContantbon cb ON vo.fldContantbonID = cb.fldContantbonID
					WHERE
						vor.fldArtikelID IS NOT null
					) mutaties
				GROUP BY mutaties.fldArtikelID) vrd ON a.fldArtikelID = vrd.fldArtikelID
WHERE a.fldNonActief=0 
  AND klant.fldRelatieID='' 
  AND xa.fldCategorie IN (1,2)
ORDER BY Artikelcode

"""
# End SQL statement with triple quotes and specify connection
,prod_db)

# Round off any numeric values to (2) decimals

df.round(2)

#Debugging
print('done')

#Write to file
df.to_csv('Stockfeed.csv', sep='\t', encoding='utf-16', index=False)
