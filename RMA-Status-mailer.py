import smtplib
import pypyodbc 
import pandas as pd
from datetime import datetime, time

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

# Specify how to receive time
now = datetime.now()
now_time = now.time()

# Check if the current time is between 08:15 and 08:45, make base export
if now_time >= time(8,15) and now_time <= time(8,45):
    base_df = pd.read_sql_query(
        """
        SELECT 
        vo.fldOrdernummer AS 'fldordernummer'
            ,CASE 
                WHEN vo.fldAfhandelStatus IN (0) THEN 'RMA ontvangen' 
                WHEN vo.fldAfhandelStatus IN (1) THEN 'Gerepareerd' 
                WHEN vo.fldAfhandelStatus IN (2) THEN 'Wacht op ontvangst' 
            END AS 'status'
        ,CAST(rverz.fldRelatiecode AS VARCHAR) + ' - ' + rverz.fldNaam AS fldverzendklant
        ,vor.fldOmschrijving AS 'fldomschrijving'
        FROM tblVerkooporder vo  
        LEFT JOIN tblRelatie AS r ON vo.fldRelatieID = r.fldRelatieID 
	    LEFT JOIN tblLand AS land1 ON r.fldLandID = land1.fldLandID 
	    LEFT JOIN tblLand AS land2 ON r.fldCorrespondentieAdresLandID = land2.fldLandID 
	    LEFT JOIN tblVerkoopfactuur vf ON vo.fldVerkoopfactuurID = vf.fldVerkoopfactuurID 
	    LEFT JOIN tblRelatie AS rverz ON vo.fldVerzendRelatieID = rverz.fldRelatieID 
	    LEFT JOIN tblLand AS lverz ON vo.fldVerzendLandID = lverz.fldLandID 
	    INNER JOIN tblVerkoopOrderRegel vor ON vor.fldVerkoopOrderID = vo.fldVerkoopOrderID 
	    LEFT JOIN tblArtikelOmzetgroep aog ON vor.fldArtikelOmzetgroepID = aog.fldArtikelOmzetgroepID 
	    LEFT JOIN tblArtikel a ON vor.fldArtikelID = a.fldArtikelID
	    LEFT JOIN tblRelatie lev ON a.fldLeverancierRelatieID = lev.fldRelatieID
	    LEFT JOIN tblLand levland ON lev.fldLandID = levland.fldLandID
	    LEFT JOIN tblX_Artikel xa ON xa.fldArtikelID = a.fldArtikelID
        WHERE vo.fldDatum BETWEEN CONVERT(DATETIME,'20180101',112) AND CONVERT(DATETIME, '20990101', 112) 
        AND vo.fldWerkbonAfgedrukt <> 0 
        AND a.fldArtikelcode = 'uitgevoerd'
        AND vf.fldDatum IS NULL
        """
  # End SQL statement with triple quotes and specify connection
    ,prod_db)

    # Write to file
    base_df.to_csv('TempExport.csv', sep=';', encoding='utf-8', index=False)

# Import data to compare to base dataframe
compare_df = pd.read_sql_query(
    """
        SELECT 
        vo.fldOrdernummer AS 'fldordernummer'
            ,CASE 
                WHEN vo.fldAfhandelStatus IN (0) THEN 'RMA ontvangen' 
                WHEN vo.fldAfhandelStatus IN (1) THEN 'Gerepareerd' 
                WHEN vo.fldAfhandelStatus IN (2) THEN 'Wacht op ontvangst' 
            END AS 'status'
        ,CAST(rverz.fldRelatiecode AS VARCHAR) + ' - ' + rverz.fldNaam AS fldverzendklant
        ,vor.fldOmschrijving AS 'fldomschrijving'
        FROM tblVerkooporder vo  
        LEFT JOIN tblRelatie AS r ON vo.fldRelatieID = r.fldRelatieID 
	    LEFT JOIN tblLand AS land1 ON r.fldLandID = land1.fldLandID 
	    LEFT JOIN tblLand AS land2 ON r.fldCorrespondentieAdresLandID = land2.fldLandID 
	    LEFT JOIN tblVerkoopfactuur vf ON vo.fldVerkoopfactuurID = vf.fldVerkoopfactuurID 
	    LEFT JOIN tblRelatie AS rverz ON vo.fldVerzendRelatieID = rverz.fldRelatieID 
	    LEFT JOIN tblLand AS lverz ON vo.fldVerzendLandID = lverz.fldLandID 
	    INNER JOIN tblVerkoopOrderRegel vor ON vor.fldVerkoopOrderID = vo.fldVerkoopOrderID 
	    LEFT JOIN tblArtikelOmzetgroep aog ON vor.fldArtikelOmzetgroepID = aog.fldArtikelOmzetgroepID 
	    LEFT JOIN tblArtikel a ON vor.fldArtikelID = a.fldArtikelID
	    LEFT JOIN tblRelatie lev ON a.fldLeverancierRelatieID = lev.fldRelatieID
	    LEFT JOIN tblLand levland ON lev.fldLandID = levland.fldLandID
	    LEFT JOIN tblX_Artikel xa ON xa.fldArtikelID = a.fldArtikelID
        WHERE vo.fldDatum BETWEEN CONVERT(DATETIME,'20180101',112) AND CONVERT(DATETIME, '20990101', 112) 
        AND vo.fldWerkbonAfgedrukt <> 0 
        AND a.fldArtikelcode = 'uitgevoerd'
        AND vf.fldDatum IS NULL
    """
  # End SQL statement with triple quotes and specify connection
    ,prod_db,)

# Import data to compare to base dataframe
overzicht_df = pd.read_sql_query(
    """
        SELECT 
        vo.fldOrdernummer AS 'fldordernummer'
        ,vo.fldDatum AS 'Datum'
        ,'RMA in behandeling'
        ,CAST(rverz.fldRelatiecode AS VARCHAR) + ' - ' + rverz.fldNaam AS fldverzendklant
        ,vor.fldOmschrijving AS 'fldomschrijving'
        FROM tblVerkooporder vo  
        LEFT JOIN tblRelatie AS r ON vo.fldRelatieID = r.fldRelatieID 
	    LEFT JOIN tblLand AS land1 ON r.fldLandID = land1.fldLandID 
	    LEFT JOIN tblLand AS land2 ON r.fldCorrespondentieAdresLandID = land2.fldLandID 
	    LEFT JOIN tblVerkoopfactuur vf ON vo.fldVerkoopfactuurID = vf.fldVerkoopfactuurID 
	    LEFT JOIN tblRelatie AS rverz ON vo.fldVerzendRelatieID = rverz.fldRelatieID 
	    LEFT JOIN tblLand AS lverz ON vo.fldVerzendLandID = lverz.fldLandID 
	    INNER JOIN tblVerkoopOrderRegel vor ON vor.fldVerkoopOrderID = vo.fldVerkoopOrderID 
	    LEFT JOIN tblArtikelOmzetgroep aog ON vor.fldArtikelOmzetgroepID = aog.fldArtikelOmzetgroepID 
	    LEFT JOIN tblArtikel a ON vor.fldArtikelID = a.fldArtikelID
	    LEFT JOIN tblRelatie lev ON a.fldLeverancierRelatieID = lev.fldRelatieID
	    LEFT JOIN tblLand levland ON lev.fldLandID = levland.fldLandID
	    LEFT JOIN tblX_Artikel xa ON xa.fldArtikelID = a.fldArtikelID
        WHERE vo.fldDatum BETWEEN CONVERT(DATETIME,'20180101',112) AND CONVERT(DATETIME, '20990101', 112) 
        AND vo.fldAfhandelStatus IN (0)
        AND vo.fldWerkbonAfgedrukt <> 0 
        AND a.fldArtikelcode = 'uitgevoerd'
        AND vf.fldDatum IS NULL
        ORDER By vo.fldDatum DESC
    """
  # End SQL statement with triple quotes and specify connection
    ,prod_db,)

# Import base dataframe as benchmark
base_df = pd.read_csv('TempExport.csv', sep=';')

# find elements in df1 that are not in df2
dif_df = base_df[(base_df['status'].isin(compare_df['status']) & base_df['fldordernummer'].isin(compare_df['fldordernummer']))]
temp_df = dif_df.append(compare_df)
final_df = temp_df.drop_duplicates(keep=False)

# Change dataframes to HTML
final_html_df = final_df.to_html(index=False, border=None)
overzicht_html_df = overzicht_df.to_html(index=False, border=None)

# If there are changes to the status, email and overwrite
if len(final_df) != 0:
    # Message sender and recipient 
    fromaddr = 'John Doe <email@address.com>'  
    toaddr  = 'John Doe <email@address.com>'
    msg = MIMEMultipart()

    # Message header
    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg['Subject'] = "Message Subject"
    msg['Date'] = formatdate(localtime=True)

    # Message content
    msgText = MIMEText(
        """
        MESSAGE
        """
        +final_html_df+
        """
        MESSAGE
        """
        +overzicht_html_df+
        """
        MESSAGE
        """, 'html')

    # Specify files
    fp = open('Signature.jpg', 'rb')
    msgImage = MIMEImage(fp.read())
    fp.close()
    msgImage.add_header('Content-ID', 'signature')   
    
    # Attach all parts to mail
    msg.attach(msgText)
    msg.attach(msgImage)

    # Mailserver settings
    server = smtplib.SMTP_SSL('mail.provider.com', 000)
    server.login('email@address.com', "Password!")
    text = msg.as_string()
    server.sendmail(fromaddr, toaddr, text)
    server.quit()

    # Debugging
    print('Mail sent')

    # Overwrite basefile
    compare_df.to_csv('TempExport.csv', sep=';', encoding='utf-8', index=False)
else:
    print('No changes')
