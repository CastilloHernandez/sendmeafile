import sqlite3
import os
import time
import shutil
import argparse
import fnmatch
import re
import itertools
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEBase import MIMEBase
from email import encoders

TIMESYMBOLS = {
	'customary'	 : ('s', 'm', 'h', 'D', 'w', 'M', 'Y'),
	'customary_ext' : ('sec', 'min', 'hour', 'day', 'week', 'month', 'year'),
}

def sendbymail(path, filename, address):
	global opt
	
	fromaddr = "mantenimiento.cualquierlavado@gmail.com"


	msg = MIMEMultipart()

	msg['From'] = fromaddr
	msg['To'] = address
	msg['Subject'] = "Enviando "+ filename

	body = "Enviando "+ path

	msg.attach(MIMEText(body, 'plain'))

	
	attachment = open(path, "rb")

	part = MIMEBase('application', 'octet-stream')
	part.set_payload((attachment).read())
	encoders.encode_base64(part)
	part.add_header('Content-Disposition', 'attachment; filename="%s"' % filename)

	msg.attach(part)

	server = smtplib.SMTP('smtp.gmail.com', 587)
	server.starttls()
	server.login(fromaddr, "NoTiene1")
	text = msg.as_string()
	server.sendmail(fromaddr, address, text)
	server.quit()

def human2seconds(s):
	init = s
	prefix= {}
	prefix['s']=1
	prefix['m']=60
	prefix['h']=3600
	prefix['D']=86400
	prefix['w']=604800
	prefix['M']=2592000
	prefix['Y']=31104000
	num = ""
	while s and s[0:1].isdigit() or s[0:1] == '.':
		num += s[0]
		s = s[1:]
	num = float(num)
	letter = s.strip()
	for name, sset in TIMESYMBOLS.items():
		if letter in sset:
			break
	else:
		raise ValueError("can't interpret %r" % init)
	return int(num * prefix[letter])

SYMBOLS = {
	'customary': ('B', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'),
	'customary_ext': ('byte', 'kilo', 'mega', 'giga', 'tera', 'peta', 'exa', 'zetta', 'iotta'),
}

def human2bytes(s):
	init = s
	num = ""
	while s and s[0:1].isdigit() or s[0:1] == '.':
		num += s[0]
		s = s[1:]
	num = float(num)
	letter = s.strip()
	for name, sset in SYMBOLS.items():
		if letter in sset:
			break
	else:
		raise ValueError("can't interpret %r" % init)
	prefix = {sset[0]:1}
	for i, s in enumerate(sset[1:]):
		prefix[s] = 1 << (i+1)*10
	return int(num * prefix[letter])

def removeIfEmpty(dir, raiz=0):
	for entry in os.listdir(dir):
		if os.path.isdir(os.path.join(dir, entry)):
			removeIfEmpty(os.path.join(dir, entry))
	if raiz == 0:
		if not os.listdir(dir):
			print 'borrando directorio vacio '+ dir
			os.rmdir(dir)

def removeEmpyFolders(origen, destino, backupset):
	dirDestino = destino + os.path.basename(os.path.split(origen)[0]) + str(backupset) + os.path.sep
	removeIfEmpty(dirDestino, 1)

def scanFolder(origen):
	global db
	global opt

	print 'origen '+ origen
	cursor = db.cursor()
	maxsize= human2bytes(opt.maxsize)
	if opt.exclude:
		excludes = list(itertools.chain(*opt.exclude))
		rexcludes = r'|'.join([fnmatch.translate(x) for x in excludes]) or r'$.'
	if opt.filter:
		filters = list(itertools.chain(*opt.filter))
		rfilters = r'|'.join([fnmatch.translate(x) for x in filters]) or r'$.'
	for root, dirs, files in os.walk(origen):
		if opt.exclude:
			dirs[:] = [d for d in dirs if not re.match(rexcludes, d)]
			files[:] = [f for f in files if not re.match(rexcludes, f)]
		if opt.filter:
			files[:] = [f for f in files if re.match(rfilters, f)]
		for file in files:
			archivoEncontrado = 0
			archivoModificado = 0
			idActual = 0
			rutaActual = str(os.path.join(root, file))
			try:
				tamanioActual = os.path.getsize(rutaActual)
			except:
				print 'error al acceder '+ rutaActual
				continue
			if tamanioActual > maxsize:
				print 'archivo demasiado grande '+ rutaActual
				continue
			cursor.execute('SELECT id FROM files WHERE path="'+ rutaActual +'"')
			for row in cursor.fetchall():
				archivoEncontrado = 1
				idActual = row[0]
			if not archivoEncontrado:
				print 'archivo nuevo '+ rutaActual
				cursor.execute('INSERT INTO files(path, name, size, lastopen) VALUES(?,?,?,?)', (rutaActual, file, tamanioActual, 0 ))
				db.commit()

parser = argparse.ArgumentParser(prog='sendmeafile')
parser.add_argument('-o', action="append", required=True)
parser.add_argument('-d', action="append", required=True)
parser.add_argument('-maxsize', default='25M')
parser.add_argument('-r', default='1Y')
parser.add_argument('-exclude', action="append", nargs='*')
parser.add_argument('-filter', action="append", nargs='*')

opt = parser.parse_args()

starttime = time.time()
db = sqlite3.connect('files.db')
db.text_factory = lambda x: unicode(x, "utf-8", "ignore")

cursor = db.cursor()
cursor.execute('CREATE TABLE IF NOT EXISTS files (id INTEGER PRIMARY KEY, path TEXT, name TEXT, size INTEGER, lastopen INTEGER)')
db.commit()

for origen in opt.o:
	if not origen.endswith(os.path.sep):
		origen = origen + os.path.sep
	scanFolder(origen)

maxsize= human2bytes(opt.maxsize)

enviado=0
if opt.exclude:
	excludes = list(itertools.chain(*opt.exclude))
	rexcludes = r'|'.join([fnmatch.translate(x) for x in excludes]) or r'$.'
if opt.filter:
	filters = list(itertools.chain(*opt.filter))
	rfilters = r'|'.join([fnmatch.translate(x) for x in filters]) or r'$.'
while enviado == 0:
	idActual=0
	cursor.execute('SELECT COUNT(*) FROM files WHERE size<'+ str(maxsize) +' AND lastopen < '+ str(time.time() - human2seconds(opt.r)))
	for row in cursor.fetchall():
		totalRows= row[0]
	cursor.execute('SELECT id, path, name FROM files WHERE size<'+ str(maxsize) +' AND lastopen < '+ str(time.time() - human2seconds(opt.r)) +' LIMIT 1 OFFSET ABS(RANDOM()) % '+ str(totalRows))
	for row in cursor.fetchall():
		idActual = row[0]
		rutaActual = row[1]
		name = row[2]
	if opt.filter:
		if not re.match(rfilters, name):
			rutaActual=''
	if opt.exclude:
		if re.match(rexcludes, name):
			rutaActual=''
	if opt.o:
		origenok=0
		for origen in opt.o:
			if not origen.endswith(os.path.sep):
				origen = origen + os.path.sep
			if rutaActual.startswith(origen):
				origenok=1
	else:
		origenok=1
	if origenok == 0:
		rutaActual=''
	if os.path.isfile(rutaActual):
		print 'Enviando '+ str(idActual) +' - '+ rutaActual
		for addr in opt.d:
			print 'Enviando a '+ addr
			try:
				sendbymail(rutaActual, name, addr)
				enviado=1
			except:
				print 'error al enviar' + rutaActual
		if enviado == 1:
			cursor.execute('UPDATE files SET lastopen=? WHERE id=?', (time.time(), idActual))
			db.commit()
	else:
		if len(rutaActual):
			print 'Archivo perdido '+ rutaActual
		cursor.execute('DELETE FROM files WHERE id='+ str(idActual))
		db.commit()



