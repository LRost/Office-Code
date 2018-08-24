import time
import smtplib
import pypyodbc 
import pandas as pd
	
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.utils import formatdate
from email import encoders

# Connect to SQL database using ODBC
prod_db = pypyodbc.connect("Driver={SQL version};"
                        "Server=Servername;"
                        "Database=Database name;"
                        "uid=ID;pwd=password")

#Complete SQL query into df (DataFrame)
df_historie = pd.read_sql_query("""

SELECT
  CAST(r1.fldRelatiecode AS INT) AS Klantnr
 ,r1.fldNaam AS Klantnaam
 ,a.fldArtikelcode AS Artikelcode
 ,a.fldOmschrijving AS Artikelomschrijving
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20180101', 112) AND CONVERT(DATETIME, '20181231', 112)THEN vor.fldAantalLeveren ELSE '0' END ) AS INT) AS 'Aantal - Totaal 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20180101', 112) AND CONVERT(DATETIME, '20181231', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Resultaat - Totaal 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20181201', 112) AND CONVERT(DATETIME, '20181231', 112)THEN vor.fldAantalLeveren ELSE '0' END ) AS INT) AS 'Aantal - Dec 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20181201', 112) AND CONVERT(DATETIME, '20181231', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Resultaat - Dec 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20181101', 112) AND CONVERT(DATETIME, '20181130', 112)THEN vor.fldAantalLeveren ELSE '0' END ) AS INT) AS'Aantal - Nov 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20181101', 112) AND CONVERT(DATETIME, '20181130', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Resultaat - Nov 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20181001', 112) AND CONVERT(DATETIME, '20181031', 112)THEN vor.fldAantalLeveren ELSE '0' END ) AS INT) AS 'Aantal - Okt 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20181001', 112) AND CONVERT(DATETIME, '20181031', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Resultaat - Okt 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20180901', 112) AND CONVERT(DATETIME, '20180930', 112)THEN vor.fldAantalLeveren ELSE '0' END ) AS INT) AS 'Aantal - Sep 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20180901', 112) AND CONVERT(DATETIME, '20180930', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Resultaat - Sep 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20180801', 112) AND CONVERT(DATETIME, '20180831', 112)THEN vor.fldAantalLeveren ELSE '0' END ) AS INT) AS'Aantal - Aug 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20180801', 112) AND CONVERT(DATETIME, '20180831', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Resultaat - Aug 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20180701', 112) AND CONVERT(DATETIME, '20180731', 112)THEN vor.fldAantalLeveren ELSE '0' END ) AS INT) AS'Aantal - Juli 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20180701', 112) AND CONVERT(DATETIME, '20180731', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Resultaat - Juli 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20180601', 112) AND CONVERT(DATETIME, '20180630', 112)THEN vor.fldAantalLeveren ELSE '0' END ) AS INT) AS'Aantal - Juni 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20180601', 112) AND CONVERT(DATETIME, '20180630', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Resultaat - Juni 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20180501', 112) AND CONVERT(DATETIME, '20180531', 112)THEN vor.fldAantalLeveren ELSE '0' END ) AS INT) AS 'Aantal - Mei 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20180501', 112) AND CONVERT(DATETIME, '20180531', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Resultaat - Mei 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20180401', 112) AND CONVERT(DATETIME, '20180430', 112)THEN vor.fldAantalLeveren ELSE '0' END ) AS INT) AS'Aantal - April 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20180401', 112) AND CONVERT(DATETIME, '20180430', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Resultaat - April 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20180301', 112) AND CONVERT(DATETIME, '20180331', 112)THEN vor.fldAantalLeveren ELSE '0' END ) AS INT) AS 'Aantal - Maart 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20180301', 112) AND CONVERT(DATETIME, '20180331', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Resultaat - Maart 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20180201', 112) AND CONVERT(DATETIME, '20180228', 112)THEN vor.fldAantalLeveren ELSE '0' END ) AS INT) AS'Aantal - Feb 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20180201', 112) AND CONVERT(DATETIME, '20180228', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Resultaat - Feb 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20180101', 112) AND CONVERT(DATETIME, '20180131', 112)THEN vor.fldAantalLeveren ELSE '0' END ) AS INT) AS 'Aantal - Jan 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20180101', 112) AND CONVERT(DATETIME, '20180131', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Resultaat - Jan 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20170101', 112) AND CONVERT(DATETIME, '20171231', 112)THEN vor.fldAantalLeveren ELSE '0' END ) AS INT) AS'Aantal - Totaal 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20170101', 112) AND CONVERT(DATETIME, '20171231', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Resultaat - Totaal 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20171201', 112) AND CONVERT(DATETIME, '20171231', 112)THEN vor.fldAantalLeveren ELSE '0' END ) AS INT) AS'Aantal - Dec 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20171201', 112) AND CONVERT(DATETIME, '20171231', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Resultaat - Dec 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20171101', 112) AND CONVERT(DATETIME, '20171130', 112)THEN vor.fldAantalLeveren ELSE '0' END ) AS INT) AS 'Aantal - Nov 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20171101', 112) AND CONVERT(DATETIME, '20171130', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Resultaat - Nov 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20171001', 112) AND CONVERT(DATETIME, '20171031', 112)THEN vor.fldAantalLeveren ELSE '0' END ) AS INT) AS'Aantal - Okt 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20171001', 112) AND CONVERT(DATETIME, '20171031', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Resultaat - Okt 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20170901', 112) AND CONVERT(DATETIME, '20170930', 112)THEN vor.fldAantalLeveren ELSE '0' END ) AS INT) AS 'Aantal - Sep 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20170901', 112) AND CONVERT(DATETIME, '20170930', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Resultaat - Sep 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20170801', 112) AND CONVERT(DATETIME, '20170831', 112)THEN vor.fldAantalLeveren ELSE '0' END ) AS INT) AS'Aantal - Aug 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20170801', 112) AND CONVERT(DATETIME, '20170831', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Resultaat - Aug 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20170701', 112) AND CONVERT(DATETIME, '20170731', 112)THEN vor.fldAantalLeveren ELSE '0' END ) AS INT) AS 'Aantal - Juli 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20170701', 112) AND CONVERT(DATETIME, '20170731', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Resultaat - Juli 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20170601', 112) AND CONVERT(DATETIME, '20170630', 112)THEN vor.fldAantalLeveren ELSE '0' END ) AS INT) AS'Aantal - Juni 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20170601', 112) AND CONVERT(DATETIME, '20170630', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Resultaat - Juni 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20170501', 112) AND CONVERT(DATETIME, '20170531', 112)THEN vor.fldAantalLeveren ELSE '0' END ) AS INT) AS 'Aantal - Mei 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20170501', 112) AND CONVERT(DATETIME, '20170531', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Resultaat - Mei 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20170401', 112) AND CONVERT(DATETIME, '20170430', 112)THEN vor.fldAantalLeveren ELSE '0' END ) AS INT) AS'Aantal - April 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20170401', 112) AND CONVERT(DATETIME, '20170430', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Resultaat - April 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20170301', 112) AND CONVERT(DATETIME, '20170331', 112)THEN vor.fldAantalLeveren ELSE '0' END ) AS INT) AS 'Aantal - Maart 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20170301', 112) AND CONVERT(DATETIME, '20170331', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Resultaat - Maart 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20170201', 112) AND CONVERT(DATETIME, '20170228', 112)THEN vor.fldAantalLeveren ELSE '0' END ) AS INT) AS'Aantal - Feb 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20170201', 112) AND CONVERT(DATETIME, '20170228', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Resultaat - Feb 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20170101', 112) AND CONVERT(DATETIME, '20170131', 112)THEN vor.fldAantalLeveren ELSE '0' END ) AS INT) AS 'Aantal - Jan 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20170101', 112) AND CONVERT(DATETIME, '20170131', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Resultaat - Jan 17'
FROM tblVerkoopOrderRegel vor
LEFT JOIN tblVerkoopOrder vo
  ON vo.fldVerkoopOrderID = vor.fldVerkoopOrderID
LEFT JOIN tblVerkoopFactuur vf
  ON vo.fldVerkoopFactuurID = vf.fldVerkoopFactuurID
LEFT JOIN tblContantBon cb
  ON vo.fldContantbonID = cb.fldContantbonID
LEFT JOIN tblRelatie r1
  ON r1.fldRelatieID = vo.fldRelatieID
LEFT JOIN tblArtikel a
  ON a.fldArtikelID = vor.fldArtikelID
LEFT JOIN tblX_Artikel xa
  ON a.fldArtikelID = xa.fldArtikelID
LEFT JOIN tblArtikelVerkoopPrijs avp
  ON a.fldArtikelID = avp.fldArtikelID
WHERE vor.fldAantalLeveren > 0
AND ISNULL(vf.fldDatum, DATEADD(dd, 0, DATEDIFF(dd, 0, cb.fldDatumTijd))) BETWEEN CONVERT(DATETIME, '20170601', 112) AND CONVERT(DATETIME, '20990704', 112)
  AND xa.fldCategorie IN(1,2,3)
  AND vf.fldOrderSjabloonVerkoopID IN (1,3,7,8)
GROUP BY r1.fldRelatiecode,r1.fldNaam,a.fldOmschrijving,a.fldArtikelcode
ORDER BY r1.fldNaam ASC, a.fldArtikelcode ASC;

"""
# End SQL statement with triple quotes and specify connection
,prod_db)

# Complete SQL query into df 2 (DataFrame 2)
df_omzet = pd.read_sql_query("""

SELECT
  CAST(r1.fldRelatiecode AS INT) AS Klantnr
 ,r1.fldNaam AS Klantnaam
 ,xa.fldMerknaam AS Artikelomzetgroep
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20180101', 112) AND CONVERT(DATETIME, '20181231', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Omzet - Totaal 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20181201', 112) AND CONVERT(DATETIME, '20181231', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Omzet - Dec 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20181101', 112) AND CONVERT(DATETIME, '20181130', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Omzet - Nov 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20181001', 112) AND CONVERT(DATETIME, '20181031', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Omzet - Okt 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20180901', 112) AND CONVERT(DATETIME, '20180930', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Omzet - Sep 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20180801', 112) AND CONVERT(DATETIME, '20990831', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Omzet - Aug 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20180701', 112) AND CONVERT(DATETIME, '20180731', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Omzet - Juli 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20180601', 112) AND CONVERT(DATETIME, '20180630', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Omzet - Juni 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20180501', 112) AND CONVERT(DATETIME, '20180531', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Omzet - Mei 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20180401', 112) AND CONVERT(DATETIME, '20180430', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Omzet - April 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20180301', 112) AND CONVERT(DATETIME, '20180331', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Omzet - Maart 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20180201', 112) AND CONVERT(DATETIME, '20180228', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Omzet - Feb 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20180101', 112) AND CONVERT(DATETIME, '20180131', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Omzet - Jan 18'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20170101', 112) AND CONVERT(DATETIME, '20171231', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Omzet - Totaal 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20171201', 112) AND CONVERT(DATETIME, '20171231', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Omzet - Dec 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20171101', 112) AND CONVERT(DATETIME, '20171130', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Omzet - Nov 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20171001', 112) AND CONVERT(DATETIME, '20171031', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Omzet - Okt 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20170901', 112) AND CONVERT(DATETIME, '20170930', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Omzet - Sep 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20170801', 112) AND CONVERT(DATETIME, '20170831', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Omzet - Aug 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20170701', 112) AND CONVERT(DATETIME, '20170731', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Omzet - Juli 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20170601', 112) AND CONVERT(DATETIME, '20170630', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Omzet - Juni 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20170501', 112) AND CONVERT(DATETIME, '20170531', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Omzet - Mei 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20170401', 112) AND CONVERT(DATETIME, '20170430', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Omzet - April 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20170301', 112) AND CONVERT(DATETIME, '20170331', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Omzet - Maart 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20170201', 112) AND CONVERT(DATETIME, '20170228', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Omzet - Feb 17'
 ,CAST(SUM(CASE WHEN vo.fldDatum BETWEEN CONVERT(DATETIME, '20170101', 112) AND CONVERT(DATETIME, '20170131', 112)THEN vor.fldBedragNaKortingen ELSE '0' END ) AS INT) AS 'Omzet - Jan 17'
FROM tblVerkoopOrderRegel vor
LEFT JOIN tblVerkoopOrder vo
  ON vo.fldVerkoopOrderID = vor.fldVerkoopOrderID
LEFT JOIN tblVerkoopFactuur vf
  ON vo.fldVerkoopFactuurID = vf.fldVerkoopFactuurID
LEFT JOIN tblContantBon cb
  ON vo.fldContantbonID = cb.fldContantbonID
LEFT JOIN tblRelatie r1
  ON r1.fldRelatieID = vo.fldRelatieID
LEFT JOIN tblArtikel a
  ON a.fldArtikelID = vor.fldArtikelID
LEFT JOIN tblX_Artikel xa
  ON a.fldArtikelID = xa.fldArtikelID
LEFT JOIN tblArtikelVerkoopPrijs avp
  ON a.fldArtikelID = avp.fldArtikelID
WHERE vor.fldAantalLeveren > 0
AND ISNULL(vf.fldDatum, DATEADD(dd, 0, DATEDIFF(dd, 0, cb.fldDatumTijd))) BETWEEN CONVERT(DATETIME, '20170101', 112) AND CONVERT(DATETIME, '20990704', 112)
  AND xa.fldCategorie IN(1,2,3)
  AND vf.fldOrderSjabloonVerkoopID IN (1,3,7,8)
GROUP BY r1.fldRelatiecode,r1.fldNaam,xa.fldMerknaam
ORDER BY Klantnr ASC,xa.fldMerknaam ASC;

"""
# End SQL statement with triple quotes and specify connection
,prod_db)

# Create a Pandas Excel writer using XlsxWriter as the engine.
writer = pd.ExcelWriter('Exports/Artikelhistorie en Omzetlijst.xlsx')

#Dataframe 1 to tab 1
df_historie.to_excel(writer,'Artikelhistorie met resultaat', index=False)

# Dataframe 2 to tab 2
df_omzet.to_excel(writer,'Omzetlijst per merk-klant', index=False)

# Write to file
writer.save()

# Debugging
print('Export done')

# Message sender and recipient 
fromaddr = 'John Doe <email@address.com>'  
toaddr  = 'John Doe <email@address.com>
msg = MIMEMultipart()'

# Message header
msg['From'] = fromaddr
msg['To'] = toaddr
msg['Subject'] = "Artikelhistorie en omzetlijst van " + time.strftime("%d/%m/%Y")
msg['Date'] = formatdate(localtime=True)

 # Message content
    msgText = MIMEText(
        """
        MESSAGE
        """, 'html')
# Specify files
fp = open('signature.jpg', 'rb')
msgImage = MIMEImage(fp.read())
fp.close()
filename = "File.xlsx"
attachment = open("Exports/File.xlsx", "rb")


# Encoding
msgImage.add_header('Content-ID', 'signature')            
part = MIMEBase('application', 'octet-stream')
part.set_payload((attachment).read())
encoders.encode_base64(part)
part.add_header('Content-Disposition', "attachment; filename= %s" % filename)

# Attach all parts to mail
msg.attach(msgText)
msg.attach(msgImage)
msg.attach(part)


# Mailserver settings
server = smtplib.SMTP_SSL('mail.provider.com', 000)
server.login('email@address.com', "Password!")
text = msg.as_string()
server.sendmail(fromaddr, toaddr, text)
server.quit()

# Debugging
print('Mail sent')
