# Permissible

Permissible is an extensible, resource-based permissions system for building web APIs in Python.

## Concepts

Each data access in your application happens through a _Resource_.

_Resources_ define _access methods_, which specify the ways in which that resource can be accessed.

Each _access method_ has a set of _permissions_, which determine the users that are permitted to perform that access.

Each _Resource_ has a _Backend_, which connects the resource with an object store, like a backend database.

## Benefits

* Auditable - every permission in the system is defined consistently as a resource access, allowing you to map out and audit the entire set of permitted actions by each user.
* Extensible - the _Resources_ and _Backends_ are easy to extend, meaning every resource access in your application can be handled consistently.  Plug-ins allow you to select different permissions behaviours to, for example, choose how to resolve the permissions of the users.
* API Integration - in the future, _Permissible_ will integrate with FastAPI to provide routes to interact with the defined resource system.


## Background

The traditional approach to web app permissions is to specify which API methods a user is permitted to call.  However, programs built this way often suffer from security issues, because...

* The system is permissive by default - if you forget to add permissions to a method, it'll be callable by anyone
* Unexpected combinations of allowed methods can lead to disallowed results - the relationship between the method and the underlying data store isn't encoded in a standard way, so it's hard to reason about the possible actions a user can take

Both of these issues are addressed by resource-level (or row-level) permissions, where a user is permitted to access a method only if they're permitted to access each required resource.  Different modes of access can be defined per resource, to let different groups of users access the same resource in the same way.

Prior work such as [FastAPI-Permissions](https://github.com/holgi/fastapi-permissions) requires, for each resource access, the program to specify the type of access (or "permission") that's going to be performed on the returned resource.  However, it has no way of enforcing that the user _actually_ accesses the resource in the requested manner.

Usually this isn't a problem because the access type is encoded within the program itself.  However, if more direct interactions with the resources is required by the users, then it would be helpful to enforce the kind of accesses associated with each resource.

_Permissible_ seeks to make constructing this kind of permissions system easy, providing extensible interfaces to make designing object models for web apps a breeze.  In the future, it may support additional features such as security policy auditing, and automatic generation of API methods to interact with the object system.
