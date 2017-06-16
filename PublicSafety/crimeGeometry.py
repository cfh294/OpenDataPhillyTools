"""
A module that contains classes pertaining to various police and crime shapes and boundaries.
Under initial development.
Code by Connor Hornibrook
"""


class PoliceArea(object):
	""" Parent class for police administrative areas. """
	def __init__(self, areaID):
		self.areaID = areaID

	def getIncidentQuery(self, inctTable, idField, condition=""):
		return """
		SELECT * FROM {table}
		WHERE {field} = '{id}'
		{condition};""".format(table=inctTable, field=idField, id=self.areaID, condition=condition)


class District(PoliceArea):
	""" Police Districts """
	def __init__(self, districtNumber, psas):
		super(District, self).__init__(districtNumber)

		self.psaList = None
		if len(psas) > 0:
			if type(psas[0]) is str:
				self.psaList = [PSA(psa) for psa in psas]
			elif type(psas[0]) is PSA:
				self.psaList = psas


class PSA(PoliceArea):
	""" PSA Areas """
	def __init__(self, psaNumber):
		super(PSA, self).__init__(psaNumber)

		try:
			self.district = psaNumber[0]
		except IndexError:
			raise CrimeGeometryError("CrimeGeometryError: PSA number not long enough.")


class CrimeGeometryError(Exception):
	""" Base exception for this module """
	def __init__(self, msg):
		super(CrimeGeometryError, self).__init__(msg)

	def __str__(self):
		return self.message
