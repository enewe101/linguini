import shutil
import unittest
from unittest import TestCase
from run import Runner, RunnerException
from task import Task, TaskException
from resource import Resource, FileResource
import os

TEST_DIR = 'temporary_testing_files'

def touch(fname):
	open(fname, 'a').close()

class TestRunner(TestCase):

	def test_null_runner(self):

		# if you don't define a lot, an exception is raised
		runner = Runner()
		with self.assertRaises(RunnerException):
			runner.run()

		# A null runner can be run, once you specify the lot
		class MyRunner(Runner):
			lot = 'my_lot'

		runner = MyRunner()
		runner.run()


	def test_run_until(self):

		class MyTask(Task):
			outputs = None
			was_run = False		# Track whether the task was run
			def exists(self):	# ensures the task will not be skipped
				return False
			def run(self):
				self.was_run = True

		task0 = MyTask()
		task1 = MyTask()
		task2 = MyTask()
		task3 = MyTask()

		class MyRunner(Runner):
			lot = 'my_lot'
			tasks = {
				'task0':task0,
				'task1':task1,
				'task2':task2,
				'task3':task3
			}
			layout = {
				'END': 'task3',
				'task3': 'task2',
				'task2': ['task1','task0']
			}

		my_runner = MyRunner()
		my_runner.run(until='task2')

		self.assertTrue(task0.was_run)
		self.assertTrue(task1.was_run)
		self.assertTrue(task2.was_run)
		self.assertFalse(task3.was_run)


	def test_run_until_classdef(self):

		class MyTask(Task):
			outputs = None
			was_run = False		# Track whether the task was run
			def exists(self):	# ensures the task will not be skipped
				return False
			def run(self):
				self.was_run = True

		task0 = MyTask()
		task1 = MyTask()
		task2 = MyTask()
		task3 = MyTask()

		class MyRunner(Runner):
			lot = 'my_lot'
			until = 'task2'
			tasks = {
				'task0':task0,
				'task1':task1,
				'task2':task2,
				'task3':task3
			}
			layout = {
				'END': 'task3',
				'task3': 'task2',
				'task2': ['task1','task0']
			}

		my_runner = MyRunner()
		my_runner.run()

		self.assertTrue(task0.was_run)
		self.assertTrue(task1.was_run)
		self.assertTrue(task2.was_run)
		self.assertFalse(task3.was_run)


	def test_run_until_end(self):

		class MyTask(Task):
			outputs = None
			was_run = False		# Track whether the task was run
			def exists(self):	# ensures the task will not be skipped
				return False
			def run(self):
				self.was_run = True

		task0 = MyTask()
		task1 = MyTask()
		task2 = MyTask()
		task3 = MyTask()

		class MyRunner(Runner):
			lot = 'my_lot'
			tasks = {
				'task0':task0,
				'task1':task1,
				'task2':task2,
				'task3':task3
			}
			layout = {
				'END': 'task3',
				'task3': 'task2',
				'task2': ['task1','task0']
			}

		my_runner = MyRunner()
		my_runner.run()

		self.assertTrue(task0.was_run)
		self.assertTrue(task1.was_run)
		self.assertTrue(task2.was_run)
		self.assertTrue(task3.was_run)


class TestCompoundRunner(TestCase):

	def setUp(self):
		os.mkdir(TEST_DIR)

	def tearDown(self):
		shutil.rmtree(TEST_DIR)

	def test_run_compound(self):
		'''
			Tests that a compound runner does run the entire thing
		'''
		
		class MyTask(Task):

			def _outputs(self):
				fname = 'test%d.txt' % self.parameters['file_num']
				return FileResource(TEST_DIR, fname)

			was_run = False		# Track whether the task was run
			def exists(self):	# ensures the task will not be skipped
				return False

			def run(self):
				fh = self.outputs.open('w')
				fh.write('yo')
				fh.close()
				self.was_run = True

		task0 = MyTask(file_num=0)
		task1 = MyTask(file_num=1)
		task2 = MyTask(file_num=2)
		task3 = MyTask(file_num=3)
		task4 = MyTask(file_num=4)
		task5 = MyTask(file_num=5)
		task6 = MyTask(file_num=6)
		task7 = MyTask(file_num=7)

		class MyRunner(Runner):
			outputs = None
			lot = 'a'
			tasks = {
				'task0':task0,
				'task1':task1,
				'task2':task2,
				'task3':task3
			}
			layout = {
				'END': 'task3',
				'task3': 'task2',
				'task2': ['task1','task0']
			}
			def exists(self):
				return False

		class MoRunner(Runner):
			outputs = None
			lot = 'b'
			tasks = {
				'task0':task4,
				'task1':task5,
				'task2':task6,
				'task3':task7
			}
			layout = {
				'END': 'task3',
				'task3': 'task2',
				'task2': ['task1','task0']
			}
			def exists(self):
				return False

		class MasterRunner(Runner):
			lot = '1'
			tasks = {
				'run0': MyRunner(),
				'run1': MoRunner()
			}
			layout = {
				'END': 'run1',
				'run1': 'run0'
			}

		my_runner = MasterRunner()
		my_runner.run()

		self.assertTrue(task0.was_run)
		self.assertTrue(task1.was_run)
		self.assertTrue(task2.was_run)
		self.assertTrue(task3.was_run)
		self.assertTrue(task4.was_run)
		self.assertTrue(task5.was_run)
		self.assertTrue(task6.was_run)
		self.assertTrue(task7.was_run)

		print os.listdir(TEST_DIR)

		expected_files = ['1.a_test%d.txt' % d for d in range(4)]
		expected_files.extend(['1.b_test%d.txt' % d for d in range(4,8)])
		self.assertItemsEqual(expected_files, os.listdir(TEST_DIR))


	def test_run_compound_partially_complete(self):
		'''
			Tests that a compound runner does runs the entire but checks
			whether the components have been run before
		'''
		
		class MyTask(Task):

			def _outputs(self):
				fname = 'test%d.txt' % self.parameters['file_num']
				return FileResource(TEST_DIR, fname)

			was_run = False		# Track whether the task was run

			def run(self):
				fh = self.outputs.open('w')
				fh.write('yo')
				fh.close()
				self.was_run = True

		task0 = MyTask(file_num=0)
		task1 = MyTask(file_num=1)
		task2 = MyTask(file_num=2)
		task3 = MyTask(file_num=3)
		task4 = MyTask(file_num=4)
		task5 = MyTask(file_num=5)
		task6 = MyTask(file_num=6)
		task7 = MyTask(file_num=7)

		class MyRunner(Runner):
			outputs = None
			lot = 'a'
			tasks = {
				'task0':task0,
				'task1':task1,
				'task2':task2,
				'task3':task3
			}
			layout = {
				'END': 'task3',
				'task3': 'task2',
				'task2': ['task1','task0']
			}
			def exists(self):
				return False

		class MoRunner(Runner):
			outputs = None
			lot = 'b'
			tasks = {
				'task0':task4,
				'task1':task5,
				'task2':task6,
				'task3':task7
			}
			layout = {
				'END': 'task3',
				'task3': 'task2',
				'task2': ['task1','task0']
			}
			def exists(self):
				return False

		my_runner = MyRunner()
		mo_runner = MoRunner()


		class MasterRunner(Runner):
			lot = '1'
			tasks = {
				'run0': my_runner,
				'run1': mo_runner
			}
			layout = {
				'END': 'run1',
				'run1': 'run0'
			}

		# make some resources so they won't be run
		touch(os.path.join(TEST_DIR, '1.a_test0.txt'))
		touch(os.path.join(TEST_DIR, '1.b_test6.txt'))

		my_runner = MasterRunner()
		my_runner.run()

		# these tasks should not have been run
		self.assertFalse(task0.was_run)
		self.assertFalse(task4.was_run)
		self.assertFalse(task5.was_run)
		self.assertFalse(task6.was_run)

		# these tasks should have been run
		self.assertTrue(task1.was_run)
		self.assertTrue(task2.was_run)
		self.assertTrue(task3.was_run)
		self.assertTrue(task7.was_run)

		# all the files should exist at this point
		expected_files = ['1.a_test%d.txt' % d for d in range(4)]
		expected_files.extend(['1.b_test%d.txt' % d for d in range(6,8)])

		# note, 1.b_test4.txt and 1.b_test5.txt never get made because
		# they aren't needed.

		self.assertItemsEqual(expected_files, os.listdir(TEST_DIR))





class TestResource(TestCase):
	

	def test_exists_not_implemented(self):
		'''
			Test that a task that doesn't define a run function raises a 
			not implemented error
		'''
		
		class MyResource(Resource):
			pass

		class MyTask(Task):
			outputs = MyResource()

			def run(self):
				print 'hello world'

		class MyRunner(Runner):
			lot = 'my_lot'
			tasks = {'my_task':MyTask()}
			layout = {'END':'my_task'}

		runner = MyRunner()
		with self.assertRaises(NotImplementedError):
			runner.run()


	def test_exists_implemented(self):
		'''
			Test that a task that doesn't define a run function raises a 
			not implemented error
		'''
		
		class MyResource(Resource):
			def exists(self):
				return True

		class MyTask(Task):
			outputs = MyResource()

			def run(self):
				print 'hello world'

		class MyRunner(Runner):
			lot = 'my_lot'
			tasks = {'my_task':MyTask()}
			layout = {'END':'my_task'}

		runner = MyRunner()
		runner.run()



class TestTask(TestCase):

	def test_no_output(self):	

		# a task has to have some outputs
		class MyTask(Task):
			def run(self):
				print 'hello world'

		class MyRunner(Runner):
			lot = 'my_lot'
			tasks = {'my_task':MyTask()}
			layout = {'END':'my_task'}

		runner = MyRunner()

		with self.assertRaises(TaskException):
			runner.run()


	def test_run_not_implemented(self):
		'''
			Test that a task that doesn't define a run function raises a 
			not implemented error
		'''
		
		class MyResource(Resource):
			def exists(self):
				return False

		class MyTask(Task):
			outputs = Resource()

		class MyRunner(Runner):
			lot = 'my_lot'
			tasks = {'my_task':MyTask()}
			layout = {'END':'my_task'}

		runner = MyRunner()

		with self.assertRaises(NotImplementedError):
			runner.run()


	def test_simple_task_gets_run(self):
		'''
			Test that a task that doesn't define a run function raises a 
			not implemented error
		'''
		
		class MyResource(Resource):
			def exists(self):
				return False

		class MyTask(Task):
			outputs = MyResource()
			was_run = False

			def run(self):
				self.was_run = True

		my_task = MyTask()

		class MyRunner(Runner):
			lot = 'my_lot'
			tasks = {'my_task':my_task}
			layout = {'END':'my_task'}

		runner = MyRunner()
		runner.run()

		self.assertTrue(my_task.was_run)


	def test_simple_task_not_run(self):
		'''
			Test that a task whose only resource already exists does not get 
			run.
		'''
		
		class MyResource(Resource):
			def exists(self):
				return True

		class MyTask(Task):
			outputs = MyResource()
			was_run = False

			def run(self):
				self.was_run = True

		my_task = MyTask()

		class MyRunner(Runner):
			lot = 'my_lot'
			tasks = {'my_task':my_task}
			layout = {'END':'my_task'}

		runner = MyRunner()
		runner.run()

		self.assertFalse(my_task.was_run)



	def test_simple_task_many_resources_dict_run(self):
		'''
			Test that a task with many resources as a list, not 
			gets run if at least one of them does not exist
		'''
		
		class Exists(Resource):
			def exists(self):
				return True

		class Noexists(Resource):
			def exists(self):
				return False

		class MyTask(Task):
			outputs = {
				'one': Exists(),
				'two': Noexists(),
				'three': Exists(),
				'four': Exists()
			}
			was_run = False

			def run(self):
				self.was_run = True

		my_task = MyTask()

		class MyRunner(Runner):
			lot = 'my_lot'
			tasks = {'my_task':my_task}
			layout = {'END':'my_task'}

		runner = MyRunner()
		runner.run()

		self.assertTrue(my_task.was_run)


	def test_simple_task_many_resources_dict_no_run(self):
		'''
			Test that a task with many resources as a list, not 
			gets run if at least one of them does not exist
		'''
		
		class Exists(Resource):
			def exists(self):
				return True

		class MyTask(Task):
			outputs = {
				'one': Exists(),
				'two': Exists(),
				'three': Exists(),
				'four': Exists()
			}
			was_run = False

			def run(self):
				self.was_run = True

		my_task = MyTask()

		class MyRunner(Runner):
			lot = 'my_lot'
			tasks = {'my_task':my_task}
			layout = {'END':'my_task'}

		runner = MyRunner()
		runner.run()

		self.assertFalse(my_task.was_run)


	def test_simple_task_many_resources_no_run(self):
		'''
			Test that a task with many resources as a list, not 
			gets run if at least one of them does not exist
		'''
		
		class Exists(Resource):
			def exists(self):
				return True


		class MyTask(Task):
			outputs = (Exists(), Exists(), Exists(), Exists())
			was_run = False

			def run(self):
				self.was_run = True

		my_task = MyTask()

		class MyRunner(Runner):
			lot = 'my_lot'
			tasks = {'my_task':my_task}
			layout = {'END':'my_task'}

		runner = MyRunner()
		runner.run()

		self.assertFalse(my_task.was_run)




	def test_simple_task_many_resources(self):
		'''
			Test that a task with many resources as a list, not 
			gets run if at least one of them does not exist
		'''
		
		class Exists(Resource):
			def exists(self):
				return True

		class Noexists(Resource):
			def exists(self):
				return False

		class MyTask(Task):
			outputs = (Exists(), Exists(), Noexists(), Exists())
			was_run = False

			def run(self):
				self.was_run = True

		my_task = MyTask()

		class MyRunner(Runner):
			lot = 'my_lot'
			tasks = {'my_task':my_task}
			layout = {'END':'my_task'}

		runner = MyRunner()
		runner.run()

		self.assertTrue(my_task.was_run)





if __name__ == '__main__':
	unittest.main()

