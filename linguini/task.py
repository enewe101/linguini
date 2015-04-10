from resource import Resource 

class TaskException(Exception):
	pass


class Task(Resource):

	parameters = {}
	inputs = None


	def __init__(self, **kwargs):
		
		# update any class-level parameters with those provided in constructor
		self.parameters = kwargs

		# make a hashable signature of the parameters.  
		# This uniquely identifies the task
		self._hashable_parameters = tuple(sorted(self.parameters.items()))

		# Ensure all parameters are hashable
		if not all(
			[isinstance(hash(v), int) for k,v in self._hashable_parameters]
		):
			raise ValueError('values used in parameters must be hashable.')


	def get_ready(self, lot, as_pilot):

		super(Task,self).get_ready(lot, as_pilot)

		# Ready the inputs
		self.input = self._inputs()
		for input in self.get_all_inputs():
			input.get_ready(lot, as_pilot)

		# Ready the outputs 
		self.outputs = self._outputs()
		for output in self.get_all_outputs():
			output.get_ready(lot, as_pilot)


	def get_all_inputs(self):

		if self.inputs is None:
			return []

		if isinstance(self.input, Resource):
			inputs = [self.input]

		elif isinstance(self.input, dict):
			inputs = self.inputs.values()

		else:
			inputs = self.inputs

		if not isinstance(inputs, (list, tuple)):
			raise TaskException(
				'inputs must be a Resource, or a list, tuple, or dict of '
				'resources.'
			)

		return inputs


	def get_all_outputs(self):

		if self.outputs is None:
			return []

		if isinstance(self.outputs, Resource):
			outputs = [self.outputs]
			
		elif isinstance(self.outputs, dict):
			outputs = self.outputs.values()

		else:
			outputs = self.outputs

		if not isinstance(outputs, (list, tuple)):
			raise TaskException(
				'outputs must be a Resource, or a list, tuple, or dict of '
				'resources.'
			)

		return outputs


	def exists(self):
		'''
			Indicates whether this Task is complete.  It's considered
			complete if all of its outputs `exist`.
		'''

		return all([o.exists() for o in self.get_all_outputs()])


	def _outputs(self):
		try:
			return self.outputs
		except AttributeError:
			raise TaskException(
				'You must define outputs for a task.  For tasks that have '
				'No output, explicitly define `outputs = None`.'
			)


	def _inputs(self):
		return self.inputs


	def _run(self, lot=None, as_pilot=False):
		self.get_ready(lot, as_pilot)
		return self.run()


	def run(self):
		raise NotImplementedError('You need to define run().')


	def __hash__(self):
		return hash((self.__class__.__name__, self._hashable_parameters))



