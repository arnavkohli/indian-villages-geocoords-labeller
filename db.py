import mysql.connector
import json

def stringify(val):
	return "'" + val + "'"

class MySQLDB:

	def __init__(self, host, user, passwd, database, *args, **kwargs):
		# self.table = table
		self.db = mysql.connector.connect(
			host=host, 
			user=user, 
			passwd=passwd, 
			database=database)
		self.cursor = self.db.cursor()
		print ('[+] Connection to DB successful')

	def insert_data(self, data, table):
		first = "insert into {} (".format(table)
		second = "values ("
		for key, val in data.items():
			if val != '-':
				first += key.lower().replace(' ', '_').replace("/", "or") + ', '
				if key not in ['latitude', 'longitude']:
					second += stringify(str(val).replace("'", "")) + ', '
				else:
					second += f"{val}, "
		first = first.strip()[:-1] + ')'
		second = second.strip()[:-1] + ')'
		query = first + " " + second
		try:
			self.cursor.execute(query)
		except mysql.connector.errors.IntegrityError:
			return False, 'duplicate entry'
			pass
		self.db.commit()
		return True, 'success'