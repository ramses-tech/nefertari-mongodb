Changelog
=========

* :release:`0.3.2 <2015-08-19>`
* :bug:`-` Fixed outdated data in nested relationships of PATCH responses

* :release:`0.3.1 <2015-07-07>`
* :bug:`-` Fixed a bug with unicode in StringField
* :bug:`-` Fixed bug with Elasticsearch re-indexing of nested relationships
* :bug:`-` Removed 'updated_at' field from engine
* :bug:`-` Disabled Elasticsearch indexing of DictField to allow storing arbitrary JSON data
* :support:`- backported` Added support for SQLA-like 'onupdate' argument

* :release:`0.3.0 <2015-06-14>`
* :support:`-` Added python3 support
* :bug:`- major` Filter-out undefined fields on document load
* :bug:`- major` Fixed bug whereby PATCHing relationship field doesn't update all relations

* :release:`0.2.3 <2015-06-05>`
* :bug:`-` Forward compatibility with nefertari releases

* :release:`0.2.2 <2015-06-03>`
* :bug:`-` Fixed password minimum length support by adding before and after validation processors
* :bug:`-` Fixed bug with Elasticsearch indexing of nested relationships
* :bug:`-` Fixed race condition in Elasticsearch indexing

* :release:`0.2.1 <2015-05-27>`
* :bug:`-` Fixed ES mapping error when values of field were all null
* :bug:`-` Fixed metaclass fields join

* :release:`0.2.0 <2015-04-07>`
* :feature:`-` Relationship indexing

* :release:`0.1.1 <2015-04-01>`

* :release:`0.1.0 <2015-04-01>`
