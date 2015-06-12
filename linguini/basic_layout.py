runner_base = '''import sys
sys.path.append('.')
from tasks.my_first_task import MyFirstTask
from tasks.my_second_task import MySecondTask
from linguini import Runner

class MyRunner(Runner):
	lot = 'my_lot'
	tasks = {
		'task1': MyFirstTask(),
		'task2': (MySecondTask(), 'task1')
	}

if __name__=='__main__':
	MyRunner().run()
'''


first_task_base = '''from linguini import Task, SimpleTask, File, Folder

class MyFirstTask(SimpleTask):
	inputs = File('.', '0.txt', static=True)
	outputs = File('.', '1.txt')

	def run(self):
		contents = self.inputs.open('r').read()
		self.outputs.open('w').write(contents[::-1]) # reverses contents'''


second_task_base = '''from linguini import Task, SimpleTask, File, Folder

class MySecondTask(SimpleTask):
	inputs = File('.', '1.txt')
	outputs = File('.', '2.txt')

	def run(self):
		contents = self.inputs.open('r').read()
		self.outputs.open('w').write(
			'\\n'.join(['\\t'*i + c for i,c in enumerate(contents)])
		)'''


input_base = '1234567'

DIRS = ['tasks', 'resources']

FILES = {
	'': [
		('run.py', runner_base),
		('0.txt', input_base)
	],

	'tasks': [
		('__init__.py', ''),
		('my_first_task.py', first_task_base),
		('my_second_task.py', second_task_base),
	]
}
		

