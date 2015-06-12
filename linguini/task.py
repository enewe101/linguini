from utils import copy, saves_args
from resource import Resource, MarkerResource

class TaskException(Exception):
	pass


class Task(Resource):

	inputs = None
	outputs = None

	@saves_args
	def __init__(self, **kwargs):
		
		# update any class-level parameters with those provided in constructor
		self.parameters = kwargs

		# make a hashable signature of the parameters.  
		# This uniquely identifies the task
		self._hashable_parameters = tuple(sorted(self.parameters.items()))

		# Ensure all parameters are hashable
		try:
			if not all(
				[isinstance(hash(v), int) for k,v in self._hashable_parameters]
			):
				raise ValueError('values used in parameters must be hashable.')

		except TypeError:
			raise ValueError('values used in parameters must be hashable.')

		super(Task, self).__init__(**kwargs)



	def get_ready(self, lot, pilot, name, clobber=False):

		super(Task,self).get_ready(lot, pilot, name, clobber)

		# Ready the inputs
		self.inputs = copy(self._inputs())
		for input in self.get_all_inputs():
			input.get_ready(
				self.get_lot(), self.get_pilot(), 'poop', self.get_clobber())

		# Ready the outputs 
		self.outputs = copy(self._outputs())
		for output in self.get_all_outputs():
			output.get_ready(
				self.get_lot(), self.get_pilot(), 'poop', self.get_clobber())


	def get_all_inputs(self):

		if self.inputs is None:
			return []

		if isinstance(self.inputs, Resource):
			inputs = [self.inputs]

		elif isinstance(self.inputs, dict):
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
			return [self.outputs]
			
		if isinstance(self.outputs, dict):
			return self.outputs.values()

		if isinstance(self.outputs, tuple):
			return list(self.outputs)

		if isinstance(self.outputs, list):
			return self.outputs

		raise TaskException(
			'outputs must be a Resource, or a list, tuple, or dict of '
			'resources.'
		)


	def exists(self):
		'''
			Indicates whether this Task is complete.  It's considered
			complete if all of its outputs `exist`.
		'''

		return all([o.exists() for o in self.get_all_outputs()])


	def _outputs(self):
		return self.outputs


	def _inputs(self):
		return self.inputs


	def _run(self, lot=None, pilot=False):
		return_val = self.run()
		self._after()
		

	def run(self):
		raise NotImplementedError('You need to define run().')


	def _after(self):
		pass


	def __hash__(self):
		return hash((self.__class__.__name__, self._hashable_parameters))



class MarkedTask(Task):

	def get_ready(self, lot, pilot, name, clobber=False):
		super(MarkedTask, self).get_ready(lot, pilot, name, clobber)

		try:
			path = self.marker_path
		except AttributeError:
			path = '.'

		fname = self.name + '.marker'
		self.marker = MarkerResource(path, fname)
		self.marker.get_ready(lot, pilot, name, clobber)


	def exists(self):
		return self.marker.exists()


	def _after(self):
		super(MarkedTask, self)._after()
		self.marker.mark()


class SimpleTask(MarkedTask):
	outputs = None
	marker_path = './linguini_markers'
