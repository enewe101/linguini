from task import Task
from resource import Resource, File

def as_list(item):
	if isinstance(item, dict):
		return item.values()

	if hasattr(item, '__iter__'):
		return list(item)

	return [item]


class RunnerException(Exception):
	pass


class Runner(Task):

	# The null runner has no tasks, but is valid
	tasks = {}
	until = None
	lot = None
	outputs = None
	inputs = None

	def _tasks(self):
		return self.tasks


	def _layout(self):
		return self.layout


	def _until(self):
		return self.until


	def get_all_outputs(self):
		'''
			Collect all the outputs from the tasks that end this runner.
			These are taken to be the outputs of the runner itself.
		'''
		return reduce(
			lambda x,y: x+self.get_task(y).get_all_outputs(),
			self.tasks.keys(),
			[]
		)

	def get_task(self, task_name):
		return as_list(self.tasks[task_name])[0]

	def get_lot(self):

		# if self.share is True, use the lot from the class definition
		if self.share or self.static:
			if self.lot is None:
				raise RunnerException(
					'lot is not defined in %s' % self.__class__.__name__)
			return self.lot

		# normally, inherit lot
		if self.inherited_lot is not None:
			return self.inherited_lot

		# otherwise instantiated lot takes precedence
		if self.instance_lot is not None:
			return self.instance_lot

		# lastly, use the lot in the class definition
		if self.lot is None:
			raise RunnerException(
				'lot is not defined in %s' % self.__class__.__name__)
		return self.lot


	def get_ready(
			self,
			lot=None,
			pilot=False,
			name='main',
			clobber=False
		):

		if self.is_ready():
			return

		self.inherited_lot = lot
		self.inherited_pilot = pilot
		self.inherited_clobber = clobber

		self.name = name

		# at this point we should have a valid lot
		if not isinstance(self.get_lot(), basestring):
			raise RunnerException(
				'lot must be string-like (in %s).' % self.__class__.__name__)

		# resolve tasks
		self.tasks = self._tasks()

		# get all the tasks ready
		for task_name in self.tasks:

			task = as_list(self.tasks[task_name])[0]

			task.get_ready(
				lot=self.get_lot(), pilot=self.get_pilot(), name=task_name, 
				clobber=self.get_clobber()
			)

		# it isn't necessary to specify a run's outputs, because they are
		# taken to be those of the END tasks
		self.outputs = self.get_all_outputs()

		# mark as ready
		self._is_ready = True


	def recursively_schedule(self, task_names):
		'''
			accepts the name of a task, or an iterable thereof, and schedules
			those among them which are not done, as well as their 
			not-done-dependencies.  Avoids double-scheduling tasks.

			if self.clobber is True, then it schedules tasks even if already 
			done.
		'''

		task_names = as_list(task_names)
		scheduled = set()

		for task_name in reversed(task_names):

			task_def = as_list(self.tasks[task_name])
			task = task_def[0]
			dependencies = task_def[1:]

			if not task.exists() or task.get_clobber():

				# add tasks to the schedule, without permitting duplicates
				scheduled.add(task_name)

				# schedule dependant jobs, if any
				try:
					scheduled |= (self.recursively_schedule(dependencies))
				except KeyError:
					pass
				
		return scheduled


	def check_schedule(self):

		checked_tasks = set()
		okay, problem = True, None
		for task_name in self.tasks:
			okay, problem = self.check_task(task_name, checked_tasks, [])
			if not okay:
				return okay, problem

		return okay, problem


	def check_task(self, task_name, checked_tasks, pending_tasks):

		okay = True
		pending_tasks.append(task_name)

		try:
			dependencies = as_list(self.tasks[task_name])[1:]
		except:
			return (
				False, 
				'%s is listed as a dependency but never defined in %s tasks'
				% (task_name, self.__class__.__name__)
			)

		# detect cyclical dependencies arising at this step
		cyclical_dependencies = [d for d in dependencies if d in pending_tasks]
		if len(cyclical_dependencies):
			cyc_d = cyclical_dependencies[0]
			entry = pending_tasks.index(cyc_d)
			cycle = pending_tasks[entry:] + [cyc_d]
			problem = 'cyclical dependency: %s' % ' -> '.join(cycle)
			return False, problem

		okay, problem = True, None
		for d in dependencies:

			# we don't need to re-check previously checked tasks
			if d in checked_tasks:
				continue

			# recurse, check this tasks dependencies
			okay, problem = self.check_task(d, checked_tasks, pending_tasks)
			if not okay:
				return okay, problem

		this_task_name = pending_tasks.pop()
		assert(this_task_name == task_name)
		checked_tasks.add(task_name)

		return okay, problem


	def run_schedule(self):

		# keep looping as long as their are incomplete tasks
		while len(self.schedule)>0:

			remaining = [t for t in self.schedule]

			# look at each task left in the schedule
			for task_name in remaining:

				# get the task definition
				task_def = as_list(self.tasks[task_name])
				task = task_def[0]
				dependencies = task_def[1:]

				# if the task has scheduled dependencies, we can't run it yet
				if any([d in self.schedule for d in dependencies]):
					continue

				# run the task, and remove it from the schedule
				task._run()
				self.schedule.remove(task_name)


	def _run(
			self, 
			lot=None,
			pilot=False,
			until=None,
			clobber=None,
			share=False
		):

		self.run(lot, pilot=pilot, until=until, clobber=clobber)


	def run(
			self, 
			lot=None,
			pilot=False,
			until=None,
			clobber=False,
			share=False
		):

		self.share = share

		self.get_ready(
			lot=lot, 
			pilot=pilot,
			name='main',
			clobber=clobber
		)

		# resolve until
		if until is not None:
			self.until = until

		elif not hasattr(self, 'until'):
			self.until = None

		elif hasattr(self, '_until'):
			self.until = self._until()

		#print 'Starting runner for lot %s.' % self.get_lot()

		# by default, run whatever is in 'END'
		until = until or self.until
		if until is None:
			until = self.tasks.keys()

		# ensure that the schedule can actually complete
		okay, problem = self.check_schedule()
		if not okay:
			raise RunnerException(problem)

		# schedule the necessary tasks
		self.schedule = self.recursively_schedule(until)

		# run the tasks
		self.run_schedule()


