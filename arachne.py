#!/usr/bin/env python
#
# The Arachne Framework provides backend storage, context search and 
# analytics support for the vextir world. The storage mechanism is 
# inspired on the graph database (process graph) structure and is 
# build on cassandra after maximising asynchronous call mechanisms
# and in the data transport layers.
#
# Authors: Rohit Jain <rohit.jain@vextir.com>
# 	   P. Santosh Kumar <santosh.pothula@vextir.com>
# 
# Date Started: 6 June 2011

import pycassa

class CassandraConnector:
	def __init__(self, keyspace, cassandra_server_list=None):
		self.keyspace = keyspace
		if cassandra_server_list is None:
			self.pool = pycassa.connect(keyspace)
		else:
			self.pool = pycassa.connect(keyspace, cassandra_server_list)

class GraphNotReadyException(Exception):
	def __str__(self):
		return repr(self)

class ArachneOperationException(Exception):
	def __init__(self, value):
		self.value = value

	def __str__(self):
		return repr(self.value)

class Arachne:
	def __init__(self, cassandra_connector):
		self.connector = cassandra_connector
		self.system_manager = pycassa.SystemManager()
		self.isGraphReady = False

	# Graph Administrative Mehods
	def constructGraph(self, graph_name):
		if self.existsGraph(graph_name):
			return False
		else:
			self.system_manager.create_column_family(self.connector.keyspace, graph_name + '_IN', super=True)
			self.system_manager.create_column_family(self.connector.keyspace, graph_name + '_OUT', super=True)
			return True
			
	def existsGraph(self, graph_name):
		cf_desc = self.system_manager.get_keyspace_column_families(self.connector.keyspace)
		try:
			cf_desc[graph_name+'_IN']
			cf_desc[graph_name+'_OUT']
		except KeyError:
			return False
		else:
			return True

	def connectGraph(self, graph_name):
		if self.existsGraph(graph_name):
			self.ingraph = pycassa.ColumnFamily(self.connector.pool, graph_name+'_IN')
			self.outgraph = pycassa.ColumnFamily(self.connector.pool, graph_name+'_OUT')
			self.isGraphReady = True
			return True
		else:
			return False

	def dropGraph(self, graph_name):
		self.system_manager.drop_column_family(self.connector.keyspace, graph_name+'_IN')
		self.system_manager.drop_column_family(self.connector.keyspace, graph_name+'_OUT')
		return True

	#Node Administrative Methods
	#Assert: self.isGraphReady=True
	#arachne is not allowed as a type
	def existsNode(self, node):
		if self.isGraphReady:
			row_key = node[1] + '_' + node[0]
			try:
				self.outgraph.get(row_key)    # Assuming Consistency of in and out graphs!!!
			except pycassa.cassandra.ttypes.NotFoundException:
				return False
			else:
				return True
		else:
			raise GraphNotReadyException

	def addNode(self, node, nodeProperties=None):
		if self.isGraphReady:
			if self.existsNode(node):
				return False
			else:
				row_key = node[1] + '_' + node[0]
				self.outgraph.insert(row_key, {'arachneNodeProperties': self.__addDefaultProperty(nodeProperties)})
				self.ingraph.insert(row_key, {'arachneNodeProperties': self.__addDefaultProperty(nodeProperties)})
		else:
			raise GraphNotReadyException

	def deleteNode(self, node):
		if self.isGraphReady:
			if self.existsNode(node):
				row_key = node[1] + '_' + node[0]
				self.outgraph.remove(row_key)
				self.ingraph.remove(row_key)
			else:
				return False
		else:
			raise GraphNotReadyException

	def updateNodeProperties(self, node, nodeProperties):
		if self.isGraphReady:
			if self.existsNode(node):
				row_key = node[1] + '_' + node[0]
				self.outgraph.insert(row_key, {'arachneNodeProperties': nodeProperties})
				self.ingraph.insert(row_key, {'arachneNodeProperties': nodeProperties})
			else:
				return False
		else:
			raise GraphNotReadyException

	def getNodeProperties(self, node):
		if self.isGraphReady:
			if self.existsNode(node):
				row_key = node[1] + '_' + node[0]
				prop = self.outgraph.get(row_key, super_column = 'arachneNodeProperties')
				return self.__removeDefaultProperty(prop)
			else:
				return None
		else:
			raise GraphNotReadyException
	
	def clearNodeProperties(self, node):
		if self.isGraphReady:
			if self.existsNode(node):
				row_key = node[1] + '_' + node[0]
				prop = self.outgraph.remove(row_key, super_column = 'arachneNodeProperties')
				prop = self.ingraph.remove(row_key, super_column = 'arachneNodeProperties')
				self.outgraph.insert(row_key, {'arachneNodeProperties': self.__addDefaultProperty(None)})
				self.ingraph.insert(row_key, {'arachneNodeProperties': self.__addDefaultProperty(None)})
			else:
				return None
		else:
			raise GraphNotReadyException
	
	def __addDefaultProperty(self, propertyDictionary):
		if propertyDictionary is None:
			return dict({'DefaultProperty':'Default'})
		propertyDictionary['DefaultProperty'] = 'Default'
		return propertyDictionary

	def __removeDefaultProperty(self, propertyDictionary):
		del propertyDictionary['DefaultProperty']
		return propertyDictionary	

	#Edge Administrative Methods
	#Assert: self.isGraphReady=True	
	def existsEdge(self, sourceNode, destinationNode, withEdgeProperties=None):
		if self.isGraphReady:
			try:
				source_key = sourceNode[1] + '_' + sourceNode[0]
				destination_key = destinationNode[1] + '_' + destinationNode[0]
				prop = self.outgraph.get(source_key, super_column = destination_key)

				if not self.existsNode(sourceNode):
					self.ingraph.remove(destination_key, super_column=source_key)
					return False
				if not self.existsNode(destinationNode):
					self.outgraph.remove(source_key, super_column=destination_key)
					return False

				if withEdgeProperties is None:
					return True
				else:
					for key,value in withEdgeProperties:
						if not(prop[key] == value):
							return False
					return True
			except pycassa.cassandra.ttypes.NotFoundException:
				return False
		else:
			raise GraphNotReadyException		

	def addEdge(self, sourceNode, destinationNode, edgeProperties=None):
		if self.isGraphReady:
			if self.existsEdge(sourceNode, destinationNode):
				return False
			else:
				source_key = sourceNode[1] + '_' + sourceNode[0]
				destination_key = destinationNode[1] + '_' + destinationNode[0]
				edgePropertiesNew = self.__addDefaultProperty(edgeProperties)
				self.outgraph.insert(source_key, {destination_key:edgePropertiesNew, destinationNode[1]+'!':{'DefaltValue':'Default'}, destinationNode[1]+'~':{'DefaultValue':'Default'}, 'arachneNodeProperties': {}})
				self.ingraph.insert(destination_key, {source_key:edgePropertiesNew, sourceNode[1]+'!':{'DefaltValue':'Default'}, sourceNode[1]+'~':{'DefaultValue':'Default'}})
		else:
			raise GraphNotReadyException

	def updateEdge(self, sourceNode, destinationNode, edgeProperties):
		if self.isGraphReady:
			if self.existsEdge(sourceNode, destinationNode):
				source_key = sourceNode[1] + '_' + sourceNode[0]
				destination_key = destinationNode[1] + '_' + destinationNode[0]
				self.outgraph.insert(source_key, {destination_key:edgeProperties})
				self.ingraph.insert(destination_key, {source_key:edgeProperties})
			else:
				return False
		else:
			raise GraphNotReadyException

	def getEdgeProperties(self, sourceNode, destinationNode):
		if self.isGraphReady:
			if self.existsEdge(sourceNode, destinationNode):
				source_key = sourceNode[1] + '_' + sourceNode[0]
				destination_key = destinationNode[1] + '_' + destinationNode[0]
				return self.__removeDefaultProperty(self.outgraph.get(source_key, super_column=destination_key))
			else:
				return None
		else:
			raise GraphNotReadyException		

	def deleteEdge(self, sourceNode, destinationNode):
		if self.isGraphReady:
			source_key = sourceNode[1] + '_' + sourceNode[0]
			destination_key = destinationNode[1] + '_' + destinationNode[0]
			self.outgraph.remove(source_key, super_column=destination_key)
			self.ingraph.remove(destination_key, super_column=source_key)
		else:
			raise GraphNotReadyException

	def clearEdgeProperties(self, sourceNode, destinationNode):
		self.deleteEdge(sourceNode, destinationNode)
		self.addEdge(sourceNode, destinationNode)



	#Additional Fuction to remove the "type_" prefix from the colum keys or row keys 
	# in the database
	def __removePrefix(self, key, Type):
		return key.replace(Type+'_', '', 1)
		
	#Graph Structure Administrative Methods
	# The following functions were designed by PSK. Blame him if something goes wrong.
	def getOutNeighbours(self, node, destinationNodeType, withEdgeProperties=None):
		if self.isGraphReady:
			try:
				buffer_start_key = destinationNodeType + '!'
				buffer_end_key = destinationNodeType + '~'
				row_key = node[1] + '_' + node[0]
				column_buffer = self.outgraph.get(row_key, column_start=buffer_start_key, column_finish=buffer_end_key, column_count=10, include_timestamp=True)
				buffer_finished = False
				while True:
					del column_buffer[buffer_start_key]
					try:
						del column_buffer[buffer_end_key]
						buffer_finished = True
					except KeyError:
						pass 
					buffer_size = len(column_buffer)
					k_last = buffer_start_key
					for k,v in column_buffer.items():
						if withEdgeProperties is None:
							yield (self.__removePrefix(k, destinationNodeType) ,self.__removeDefaultProperty(v))
						else:
							for l,m in withEdgeProperties.items():
								if not (v[l] is  m):
									break
							else:
								yield (self.__removePrefix(k, destinationNodeType),self.__removeDefaultProperty(v))
						k_last = k
					if buffer_finished:
						break
					buffer_start_key = k_last	
					column_buffer = self.outgraph.get(row_key, column_start=buffer_start_key, column_finish=buffer_end_key, column_count=10, include_timestamp=True)
			except pycassa.cassandra.ttypes.NotFoundException:
				pass
		else:
			raise GraphNotReadyException

	def getInNeighbours(self, node, sourceNodeType, withEdgeProperties=None):
		if self.isGraphReady:
			try:
				buffer_start_key = sourceNodeType + '!'
				buffer_end_key = sourceNodeType + '~'
				row_key = node[1] + '_' + node[0]
				column_buffer = self.ingraph.get(row_key, column_start=buffer_start_key, column_finish=buffer_end_key, column_count=10, include_timestamp=True)
				buffer_finished = False
				while True:
					del column_buffer[buffer_start_key]
					try:
						del column_buffer[buffer_end_key]
						buffer_finished = True
					except KeyError:
						pass 
					buffer_size = len(column_buffer)
					k_last = buffer_start_key
					for k,v in column_buffer.items():
						if withEdgeProperties is None:
							yield (self.__removePrefix(k, sourceNodeType),self.__removeDefaultProperty(v))
						else:
							for l,m in withEdgeProperties.items():
								if not (v[l] is  m):
									break
							else:
								yield (self.__removePrefix(k, sourceNodeType),self.__removeDefaultProperty(v))
						k_last = k
					if buffer_finished:
						break
					buffer_start_key = k_last	
					column_buffer = self.ingraph.get(row_key, column_start=buffer_start_key, column_finish=buffer_end_key, column_count=10, include_timestamp=True)
			except pycassa.cassandra.ttypes.NotFoundException:
				pass
		else:
			raise GraphNotReadyException
