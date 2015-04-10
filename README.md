# What it does #

 - enables specifying how tasks depend on one another in one location
 - enables specifying how tasks depend on resources
 - based on these dependencies, allows executing the entire pipeline, or 
	executing through to a particular task
 - allows you to run variations of your pipeline, while keeping track of 
	which resources belong to which runs
 - allows you to build up pipelines from sub-pipelines
 - only executing tasks that are needed, so executing the last step in
	a pipeline is the same command as executing the full pipeline

# How? #

 - To architect the overall pipeline, you subclass a special Runner class 
	specifying a tasks dictionary and a layout dictionary using a declarative 
	style.
 - the `tasks` dictionary names all the tasks that are involved in the run
	and the `layout` dictionary establishes how they depend on one another
 - Even when runs involve all the same tasks, they might have different results
	because the tasks can be parametrized.  They can easily be told apart
	based on a unique string, the `lot`, specified on the runner.
 - To specify given tasks in your pipeline, you subclass a special Task
	class.  Tasks have to define a `run()` function, which has to accept
	a keyword argument `lot`.
 - Tasks indicate what resources they require, and what resources they yield
	based on two dictionaries: `input` and `output`. 
 - To specify a resource, you subclass a special Resource class.  Resource
	classes accept a `lot` argument in their `__init__` method, and accept 
	an optional `as_pilot` argument.  Two resources that have different
	values for `lot` and `as_pilot` should not overwrite eachother's data 
	nor interfere whith one another's return values for `ready` 
 - Resources must define a `ready` method, which returns True if the resource
	is completely ready, and False otherwise.
 - A task is considered complete if all the resources in its output are ready.
 - A task is considered ready if all the tasks upon which it depends are
	complete.
 - Note, tasks are instantiated within their Runner, and resources
	are instantiated within their Task is instantiated
