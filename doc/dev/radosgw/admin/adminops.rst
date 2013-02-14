==================
 Admin Operations
==================

An admin API request will be done on a URI that starts with the configurable 'admin'
resource entry point. Authorization for the admin API duplicates the S3 authorization
mechanism. Some operations require that the user holds special administrative capabilities.
The response entity type (XML or JSON) may be specified as the 'format' option in the
request and defaults to JSON if not specified.

Get Usage
=========

Request bandwidth usage information.

Syntax
~~~~~~

::

	GET /{admin}/usage?format=json HTTP/1.1 
	Host: {fqdn}



Request Parameters
~~~~~~~~~~~~~~~~~~

``uid``

:Description: The user for which the information is requested. If not specified will apply to all users.
:Type: String
:Example: ``foo_user``
:Required: No

``start``

:Description: Date and (optional) time that specifies the start time of the requested data.
:Type: String
:Example: ``2012-09-25 16:00:00``
:Required: No

``end``

:Description: Date and (optional) time that specifies the end time of the requested data (non-inclusive).
:Type: String
:Example: ``2012-09-25 16:00:00``
:Required: No


``show-entries``

:Description: Specifies whether data entries should be returned.
:Type: Boolean
:Example: True [False]
:Required: No


``show-summary``

:Description: Specifies whether data summary should be returned.
:Type: Boolean
:Example: True [False]
:Required: No



Response Entities
~~~~~~~~~~~~~~~~~

If successful, the response contains the requested information.

``usage``

:Description: A container for the usage information.
:Type: Container

``entries``

:Description: A container for the usage entries information.
:Type: Container

``user``

:Description: A container for the user data information.
:Type: Container

``owner``

:Description: The name of the user that owns the buckets.
:Type: String

``bucket``

:Description: The bucket name.
:Type: String

``time``

:Description: Time lower bound for which data is being specified (rounded to the beginning of the first relevant hour).
:Type: String

``epoch``

:Description: The time specified in seconds since 1/1/1970.
:Type: String

``categories``

:Description: A container for stats categories.
:Type: Container

``entry``

:Description: A container for stats entry.
:Type: Container

``category``

:Description: Name of request category for which the stats are provided.
:Type: String

``bytes_sent``

:Description: Number of bytes sent by the RADOS Gateway.
:Type: Integer

``bytes_received``

:Description: Number of bytes received by the RADOS Gateway.
:Type: Integer

``ops``

:Description: Number of operations.
:Type: Integer

``successful_ops``

:Description: Number of successful operations.
:Type: Integer

``summary``

:Description: A container for stats summary.
:Type: Container

``total``

:Description: A container for stats summary aggregated total.
:Type: Container

Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~

TBD.

Trim Usage
==========

Remove usage information. With no dates specified, removes all usage 
information. 

Syntax
~~~~~~

::

	DELETE /{admin}/usage?format=json HTTP/1.1
	Host: {fqdn}



Request Parameters
~~~~~~~~~~~~~~~~~~

``uid``

:Description: The user for which the information is requested. If not specified will apply to all users.
:Type: String
:Example: ``foo_user``
:Required: No

``start``

:Description: Date and (optional) time that specifies the start time of the requested data.
:Type: String
:Example: ``2012-09-25 16:00:00``
:Required: No

``end``

:Description: Date and (optional) time that specifies the end time of the requested data (none inclusive).
:Type: String
:Example: ``2012-09-25 16:00:00``
:Required: No


``remove-all``

:Description: Required when uid is not specified, in order to acknowledge multi user data removal.
:Type: Boolean
:Example: True [False]
:Required: No

Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~

TBD.

Get User Info
=============

Get user information. If no user is specified returns the list of all users along with suspension 
information.


Syntax
~~~~~~

::

	GET /{admin}/user?format=json HTTP/1.1
	Host: {fqdn}


Request Parameters
~~~~~~~~~~~~~~~~~~

``uid``

:Description: The user for which the information is requested.
:Type: String
:Example: ``foo_user``
:Required: No


Response Entities
~~~~~~~~~~~~~~~~~

If successful, the response contains the user information.

``user``

:Description: A container for the user data information.
:Type: Container

``user_id``

:Description: The user id.
:Type: String
:Parent: ``user``

``display_name``

:Description: Display name for the user.
:Type: String
:Parent: ``user``

``suspended``

:Description: True if the user is suspended.
:Type: Boolean
:Parent: ``user``

``max_buckets``

:Description: The maximum number of buckets to be owned by the user.
:Type: Integer
:Parent: ``user``

``subusers``

:Description: Subusers associated with this user account.
:Type: Container
:Parent: ``user``

``keys``

:Description: S3 keys associated with this user account.
:Type: Container
:Parent: ``user``

``swift_keys``

:Description: Swift keys associated with this user account.
:Type: Container
:Parent: ``user``

``caps``

:Description: User capabilities.
:Type: Container
:Parent: ``user``

Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~

None.

Create User
===========

Create a new user. By Default, a S3 key pair will be created automatically 
and returned in the response. If only one of ``access-key`` or ``secret-key``
is provided, the omitted key will be automatically generated. By default, a
generated key is added to the keyring without replacing an existing key pair.
If ``access-key`` is specified and refers to an existing key owned by the user
then it will be modified.


Syntax
~~~~~~

::

	PUT /{admin}/user?format=json HTTP/1.1
	Host: {fqdn}



Request Parameters
~~~~~~~~~~~~~~~~~~

``uid``

:Description: The user ID to be created.
:Type: String
:Example: ``foo_user``
:Required: Yes

``display-name``

:Description: The display name of the user to be created.
:Type: String
:Example: ``foo user``
:Required: Yes


``email``

:Description: The email address associated with the user.
:Type: String
:Example: ``foo@bar.com``
:Required: No

``key-type``

:Description: Key type to be generated, options are: swift, s3 (default).
:Type: String
:Example: ``s3`` [``s3``]
:Required: No

``access-key``

:Description: Specify access key.
:Type: String
:Example: ``ABCD0EF12GHIJ2K34LMN``
:Required: No


``secret-key``

:Description: Specify secret key.
:Type: String
:Example: ``0AbCDEFg1h2i34JklM5nop6QrSTUV+WxyzaBC7D8``
:Required: No

``caps``

:Description: User capabilities.
:Type: String
:Example: ``usage=read, write; user=read``
:Required: No

``generate-key``

:Description: Generate a new key pair and add to the existing keyring.
:Type: Boolean
:Example: True [True]
:Required: No

``max-buckets``

:Description: Specify the maximum number of buckets the user can own.
:Type: Integer
:Example: 500 [1000]
:Required: No

Response Entities
~~~~~~~~~~~~~~~~~

If successful, the response contains the user information.

``user``

:Description: A container for the user data information.
:Type: Container

``user_id``

:Description: The user id.
:Type: String
:Parent: ``user``

``display_name``

:Description: Display name for the user.
:Type: String
:Parent: ``user``

``suspended``

:Description: True if the user is suspended.
:Type: Boolean
:Parent: ``user``

``max_buckets``

:Description: The maximum number of buckets to be owned by the user.
:Type: Integer
:Parent: ``user``

``subusers``

:Description: Subusers associated with this user account.
:Type: Container
:Parent: ``user``

``keys``

:Description: S3 keys associated with this user account.
:Type: Container
:Parent: ``user``

``swift_keys``

:Description: Swift keys associated with this user account.
:Type: Container
:Parent: ``user``

``caps``

:Description: User capabilities.
:Type: Container
:Parent: ``user``

Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~

``UserExists``

:Description: Attempt to create existing user.
:Code: 409 Conflict

``InvalidAccessKey``

:Description: Invalid access key specified.
:Code: 400 Bad Request

``InvalidKeyType``

:Description: Invalid key type specified.
:Code: 400 Bad Request

``InvalidSecretKey``

:Description: Invalid secret key specified.
:Code: 400 Bad Request

``InvalidKeyType``

:Description: Invalid key type specified.
:Code: 400 Bad Request

``KeyExists``

:Description: Provided access key exists and belongs to another user.
:Code: 409 Conflict

``EmailExists``

:Description: Provided email address exists.
:Code: 409 Conflict

``InvalidCap``

:Description: Attempt to grant invalid admin capability.
:Code: 400 Bad Request


Modify User
===========

Modify a user.

Syntax
~~~~~~

::

	POST /{admin}/user?format=json HTTP/1.1
	Host: {fqdn}


Request Parameters
~~~~~~~~~~~~~~~~~~

``uid``

:Description: The user ID to be modified.
:Type: String
:Example: ``foo_user``
:Required: Yes

``display-name``

:Description: The display name of the user to be modified.
:Type: String
:Example: ``foo user``
:Required: No

``email``

:Description: The email address to be associated with the user.
:Type: String
:Example: ``foo@bar.com``
:Required: No

``generate-key``

:Description: Generate a new key pair and add to the existing keyring.
:Type: Boolean
:Example: True [False]
:Required: No

``key-type``

:Description: Key type to be generated, options are: swift, s3 (default).
:Type: String
:Example: ``s3``
:Required: No

``caps``

:Description: User capabilities.
:Type: String
:Example: ``usage=read, write; user=read``
:Required: No

Response Entities
~~~~~~~~~~~~~~~~~

If successful, the response contains the user information.

``user``

:Description: A container for the user data information.
:Type: Container

``user_id``

:Description: The user id.
:Type: String
:Parent: ``user``

``display_name``

:Description: Display name for the user.
:Type: String
:Parent: ``user``


``suspended``

:Description: True if the user is suspended.
:Type: Boolean
:Parent: ``user``


``max_buckets``

:Description: The maximum number of buckets to be owned by the user.
:Type: Integer
:Parent: ``user``


``subusers``

:Description: Subusers associated with this user account.
:Type: Container
:Parent: ``user``


``keys``

:Description: S3 keys associated with this user account.
:Type: Container
:Parent: ``user``


``swift_keys``

:Description: Swift keys associated with this user account.
:Type: Container
:Parent: ``user``


``caps``

:Description: User capabilities.
:Type: Container
:Parent: ``user``


Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~

``InvalidAccessKey``

:Description: Invalid access key specified.
:Code: 400 Bad Request

``InvalidKeyType``

:Description: Invalid key type specified.
:Code: 400 Bad Request

``InvalidSecretKey``

:Description: Invalid secret key specified.
:Code: 400 Bad Request

``KeyExists``

:Description: Provided access key exists and belongs to another user.
:Code: 409 Conflict

``EmailExists``

:Description: Provided email address exists.
:Code: 409 Conflict

``InvalidCap``

:Description: Attempt to grant invalid admin capability.
:Code: 400 Bad Request

Remove User
===========

Remove an existing user.

Syntax
~~~~~~

::

	DELETE /{admin}/user?format=json HTTP/1.1
	Host: {fqdn}


Request Parameters
~~~~~~~~~~~~~~~~~~

``uid``

:Description: The user ID to be removed.
:Type: String
:Example: ``foo_user``
:Required: Yes.

``purge-data``

:Description: When specified the buckets and objects belonging
              to the user will also be removed.
:Type: Boolean
:Example: True
:Required: No

Response Entities
~~~~~~~~~~~~~~~~~

None

Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~

None.

Create Subuser
==============

Create a new subuser (primarily useful for clients using the Swift API).
Note that either ``gen-subuser`` or ``subuser`` is required for a valid
request. Note that in general for a subuser to be useful, it must be
granted permissions by specifying ``access``. As with user creation if
``subuser`` is specified without ``secret``, then a secret key will
be automatically generated.

Syntax
~~~~~~

::

	PUT /{admin}/user?subuser?format=json HTTP/1.1
	Host {fqdn}


Request Parameters
~~~~~~~~~~~~~~~~~~

``uid``

:Description: The user ID under which a subuser is to  be created.
:Type: String
:Example: ``foo_user``
:Required: Yes


``subuser``

:Description: Specify the subuser ID to be created.
:Type: String
:Example: ``sub_foo``
:Required: No

``generate-subuser``

:Description: Generate the subuser automatically.
:Type: Boolean
:Example: True [False]
:Required: No

``secret``

:Description: Specify secret key.
:Type: String
:Example: ``0AbCDEFg1h2i34JklM5nop6QrSTUV+WxyzaBC7D8``
:Required: No

``key-type``

:Description: Key type to be generated, options are: swift (default), s3.
:Type: String
:Example: ``swift`` [``swift``]
:Required: No

``access``

:Description: Set access permissions for sub-user, should be one
              of ``read, write, readwrite, full``.
:Type: String
:Example: ``read``
:Required: No


Response Entities
~~~~~~~~~~~~~~~~~

If successful, the response contains the user information.

``user``

:Description: A container for the user data information.
:Type: Container

``user_id``

:Description: The user id.
:Type: String
:Parent: ``user``

``display_name``

:Description: Display name for the user.
:Type: String
:Parent: ``user``


``suspended``

:Description: True if the user is suspended.
:Type: Boolean
:Parent: ``user``

``max_buckets``

:Description: The maximum number of buckets to be owned by the user.
:Type: Integer
:Parent: ``user``

``subusers``

:Description: Subusers associated with the user account.
:Type: Container
:Parent: ``user``

``keys``

:Description: S3 keys associated with the user account.
:Type: Container
:Parent: ``user``

``swift_keys``

:Description: Swift keys associated with the user account.
:Type: Container
:Parent: ``user``

``caps``

:Description: User capabilities.
:Type: Container
:Parent: ``user``

Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~

``SubuserExists``

:Description: Specified subuser exists.
:Code: 409 Conflict

``InvalidKeyType``

:Description: Invalid key type specified.
:Code: 400 Bad Request

``InvalidSecretKey``

:Description: Invalid secret key specified.
:Code: 400 Bad Request

``InvalidAccess``

:Description: Invalid subuser access specified.
:Code: 400 Bad Request

Modify Subuser
==============

Modify an existing subuser

Syntax
~~~~~~

::

	POST /{admin}/user?subuser?format=json HTTP/1.1
	Host {fqdn}


Request Parameters
~~~~~~~~~~~~~~~~~~

``uid``

:Description: The user ID under which the subuser is to be modified.
:Type: String
:Example: ``foo_user``
:Required: Yes

``subuser``

:Description: The subuser ID to be modified.
:Type: String
:Example: ``sub_foo``
:Required: Yes

``generate-secret``

:Description: Generate a new secret key for the subuser,
              replacing the existing key.
:Type: Boolean
:Example: True [False]
:Required: No

``secret``

:Description: Specify secret key.
:Type: String
:Example: ``0AbCDEFg1h2i34JklM5nop6QrSTUV+WxyzaBC7D8``
:Required: No

``key-type``

:Description: Key type to be generated, options are: swift (default), s3 .
:Type: String
:Example: ``swift`` [``swift``]
:Required: No

``access``

:Description: Set access permissions for sub-user, should be one
              of ``read, write, readwrite, full``.
:Type: String
:Example: ``read``
:Required: No


Response Entities
~~~~~~~~~~~~~~~~~

If successful, the response contains the user information.

``user``

:Description: A container for the user data information.
:Type: Container

``user_id``

:Description: The user id.
:Type: String
:Parent: ``user``

``display_name``

:Description: Display name for the user.
:Type: String
:Parent: ``user``

``suspended``

:Description: True if the user is suspended.
:Type: Boolean
:Parent: ``user``

``max_buckets``

:Description: The maximum number of buckets to be owned by the user.
:Type: Integer
:Parent: ``user``

``subusers``

:Description: Subusers associated with this user account.
:Type: Container
:Parent: ``user``

``keys``

:Description: S3 keys associated with this user account.
:Type: Container
:Parent: ``user``

``swift_keys``

:Description: Swift keys associated with this user account.
:Type: Container
:Parent: ``user``

``caps``

:Description: User capabilities.
:Type: Container
:Parent: ``user``

Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~

``SubuserExists``

:Description: Specified subuser exists.
:Code: 409 Conflict

``InvalidKeyType``

:Description: Invalid key type specified.
:Code: 400 Bad Request

``InvalidSecretKey``

:Description: Invalid secret key specified.
:Code: 400 Bad Request

``InvalidAccess``

:Description: Invalid subuser access specified.
:Code: 400 Bad Request

Remove Subuser
==============

Remove an existing subuser

Syntax
~~~~~~

::

	DELETE /{admin}/user?subuser?format=json HTTP/1.1
	Host {fqdn}


Request Parameters
~~~~~~~~~~~~~~~~~~

``uid``

:Description: The user ID under which the subuser is to be removed.
:Type: String
:Example: ``foo_user``
:Required: Yes


``subuser``

:Description: The subuser ID to be removed.
:Type: String
:Example: ``sub_foo``
:Required: Yes

``purge-keys``

:Description: Remove keys belonging to the subuser.
:Type: Boolean
:Example: True [True]
:Required: No

``purge-data``

:Description: Remove data belonging to the subuser.
:Type: Boolean
:Example: True [False]
:Required: No

Response Entities
~~~~~~~~~~~~~~~~~

If successful, the response contains the user information.

``user``

:Description: A container for the user data information.
:Type: Container
:Parent: ``user``

``user_id``

:Description: The user id.
:Type: String
:Parent: ``user``

``display_name``

:Description: Display name for the user.
:Type: String
:Parent: ``user``

``suspended``

:Description: True if the user is suspended.
:Type: Boolean
:Parent: ``user``

``max_buckets``

:Description: The maximum number of buckets to be owned by the user.
:Type: Integer
:Parent: ``user``

``subusers``

:Description: Subusers associated with this user account.
:Type: Container
:Parent: ``user``

``keys``

:Description: S3 keys associated with this user account.
:Type: Container
:Parent: ``user``

``swift_keys``

:Description: Swift keys associated with this user account.
:Type: Container
:Parent: ``user``

``caps``

:Description: User capabilities.
:Type: Container
:Parent: ``user``

Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~
None.

Create Key
==========

Create a new key. If a ``subuser`` is specified then by default created keys
will be swift type. If only one of ``access-key`` or ``secret-key`` is provided the
committed key will be automatically generated, that is if only ``secret-key`` is
specified then ``access-key`` will be automatically generated. By default, a
generated key is added to the keyring without replacing an existing key pair.
If ``access-key`` is specified and refers to an existing key owned by the user
then it will be modified. The response is a container listing all keys of the same
type as the key created. Note that when creating a swift key, specifying the option
``access-key`` will have no effect. Additionally, only one swift key may be held by
each user or subuser.


Syntax
~~~~~~

::

	PUT /{admin}/user?key?format=json HTTP/1.1
	Host {fqdn}


Request Parameters
~~~~~~~~~~~~~~~~~~

``uid``

:Description: The user ID to receive the new key.
:Type: String
:Example: ``foo_user``
:Required: Yes

``subuser``

:Description: The subuser ID to receive the new key.
:Type: String
:Example: ``sub_foo``
:Required: No

``key-type``

:Description: Key type to be generated, options are: swift, s3 (default).
:Type: String
:Example: ``s3`` [``s3``]
:Required: No

``access-key``

:Description: Specify the access key. 
:Type: String
:Example: ``AB01C2D3EF45G6H7IJ8K``
:Required: No

``secret-key``

:Description: Specify the secret key.
:Type: String
:Example: ``0ab/CdeFGhij1klmnopqRSTUv1WxyZabcDEFgHij``
:Required: No

``generate-key``

:Description: Generate a new key pair and add to the existing keyring.
:Type: Boolean
:Example: True [``True``]
:Required: No


Response Entities
~~~~~~~~~~~~~~~~~

``keys``

:Description: Keys of type created associated with this user account.
:Type: Container

``user``

:Description: The user account associated with the key.
:Type: String
:Parent: ``keys``

``access-key``

:Description: The access key.
:Type: String
:Parent: ``keys``

``secret-key``

:Description: The secret key
:Type: String
:Parent: ``keys``


Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~

``InvalidAccessKey``

:Description: Invalid access key specified.
:Code: 400 Bad Request

``InvalidKeyType``

:Description: Invalid key type specified.
:Code: 400 Bad Request

``InvalidSecretKey``

:Description: Invalid secret key specified.
:Code: 400 Bad Request

``InvalidKeyType``

:Description: Invalid key type specified.
:Code: 400 Bad Request

``KeyExists``

:Description: Provided access key exists and belongs to another user.
:Code: 409 Conflict

Remove Key
==========

Remove an existing key.

Syntax
~~~~~~

::

	DELETE /{admin}/user?key?format=json HTTP/1.1
	Host {fqdn}


Request Parameters
~~~~~~~~~~~~~~~~~~

``access-key``

:Description: The S3 access key belonging to the S3 key pair to remove.
:Type: String
:Example: ``AB01C2D3EF45G6H7IJ8K``
:Required: Yes

``uid``

:Description: The user to remove the key from.
:Type: String
:Example: ``foo_user``
:Required: No

``subuser``

:Description: The subuser to remove the key from.
:Type: String
:Example: ``sub_foo``
:Required: No

``key-type``

:Description: Key type to be removed, options are: swift, s3.
              NOTE: Required to remove swift key.
:Type: String
:Example: ``swift``
:Required: No

Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~

None.

Response Entities
~~~~~~~~~~~~~~~~~

``keys``

:Description: Remaining keys of type removed associated with this user account.
:Type: Container

``user``

:Description: The user account associated with the key.
:Type: String
:Parent: ``keys``

``access-key``

:Description: The access key.
:Type: String
:Parent: ``keys``

``secret-key``

:Description: The secret key.
:Type: String
:Parent: ``keys``

Get Bucket Info
===============

Get information for a single existing bucket.

Syntax
~~~~~~

::

	GET /{admin}/bucket?format=json HTTP/1.1
	Host {fqdn}


Request Parameters
~~~~~~~~~~~~~~~~~~

``bucket``

:Description: The bucket to return info on.
:Type: String
:Example: ``foo_bucket``
:Required: Yes


``list``

:Description: Return list of buckets.
:Type: Boolean
:Example: True [True]
:Required: No

``stats``

:Description: Return bucket statistics.
:Type: Boolean
:Example: True [False]
:Required: No

``check``

:Description: Check bucket index.
:Type: Boolean
:Example: False [False]
:Required: No

``fix``

:Description: Also fix the bucket index when checking.
:Type: Boolean
:Example: False [False]
:Required: No

Response Entities
~~~~~~~~~~~~~~~~~

If successful the request returns a buckets container containing
the desired bucket information.

``stats``

:Description: Per bucket information.
:Type: Container

``buckets``

:Description: Contains a list of one or more bucket containers.
:Type: Container

``bucket``

:Description: Container for single bucket information.
:Type: Container
:Parent: ``buckets``

``name``

:Description: The name of the bucket.
:Type: String
:Parent: ``bucket``

``pool``

:Description: The pool the bucket is stored in.
:Type: String
:Parent: ``bucket``

``id``

:Description: The unique bucket id.
:Type: String
:Parent: ``bucket``

``marker``

:Description: Internal bucket tag.
:Type: String
:Parent: ``bucket``

``owner``

:Description: The user id of the bucket owner.
:Type: String
:Parent: ``bucket``

``usage``

:Description: Storage usage information.
:Type: Container
:Parent: ``bucket``

``index``

:Description: Status of bucket index.
:Type: String
:Parent: ``bucket``

Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~

``IndexRepairFailed``

:Description: Bucket index repair failed.
:Code: 409 Conflict

Get User Bucket Info
====================

Get information for an existing bucket belonging to a specified user.
If no request parameters are included lists buckets belonging to the
specified user. The ``buckets`` container will contain a list of
``bucket`` containers in which only the ``name`` field appears. By
specifying ``uid`` as the configurable {orphaned-bucket-owner} information
pertaining to orphaned buckets can be retrieved.

Syntax
~~~~~~

::

	GET /{admin}/bucket?format=json HTTP/1.1
	Host {fqdn}


Request Parameters
~~~~~~~~~~~~~~~~~~

``uid``

:Description: The user to retrieve bucket information for.
:Type: String
:Example: ``foo_user``
:Required: Yes

``bucket``

:Description: The bucket to return info on.
:Type: String
:Example: ``foo_bucket``
:Required: No


``list``

:Description: Return list of buckets.
:Type: Boolean
:Example: True [True]
:Required: No

``stats``

:Description: Return bucket statistics.
:Type: Boolean
:Example: True [False]
:Required: No

Response Entities
~~~~~~~~~~~~~~~~~

If successful the request returns a buckets container containing
the desired bucket information.

``stats``

:Description: Per bucket information.
:Type: Container

``buckets``

:Description: Contains a list of one or more bucket containers.
:Type: Container

``bucket``

:Description: Container for bucket information.
:Type: Container
:Parent: ``buckets``

``name``
:Description: The name of the bucket.
:Type: String
:Parent: ``bucket``

``pool``

:Description: The pool the bucket is stored in.
:Type: String

``id``

:Description: The unique bucket id.
:Type: String
:Parent: ``bucket``

``marker``

:Description:
:Type: String
:Parent: ``bucket``

``owner``

:Description: The user id of the bucket owner.
:Type: String
:Parent: ``bucket``

``usage``

:Description: Storage usage information.
:Type: Container
:Parent: ``bucket``

``index``

:Description: Status of bucket index.
:Type: String
:Parent: ``bucket``

Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~

``IndexRepairFailed``

:Description: Bucket index repair failed.
:Code: 409 Conflict

Check Bucket Index
==================

Check the index of an existing bucket.

Syntax
~~~~~~

::

	GET /{admin}/bucket?index?format=json HTTP/1.1
	Host {fqdn}


Request Parameters
~~~~~~~~~~~~~~~~~~

``bucket``

:Description: The bucket to return info on.
:Type: String
:Example: ``foo_bucket``
:Required: Yes

``fix``

:Description: Also fix the bucket index when checking.
:Type: Boolean
:Example: False [False]
:Required: No

Response Entities
~~~~~~~~~~~~~~~~~

``index``

:Description: Status of bucket index.
:Type: String

Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~

``IndexRepairFailed``

:Description: Bucket index repair failed.
:Code: 409 Conflict

Remove Bucket
=============

Delete an existing bucket.

Syntax
~~~~~~

::

	DELETE /{admin}/bucket?format=json HTTP/1.1
	Host {fqdn}



Request Parameters
~~~~~~~~~~~~~~~~~~

``bucket``

:Description: The bucket to remove.
:Type: String
:Example: ``foo_bucket``
:Required: Yes

``purge-objects``

:Description: Remove a buckets objects before deletion.
:Type: Boolean
:Example: True [False]
:Required: No

Response Entities
~~~~~~~~~~~~~~~~~

None.

Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~

``BucketNotEmpty``

:Description: Attempted to delete non-empty bucket.
:Code: 409 Conflict

``ObjectRemovalFailed``

:Description: Unable to remove objects.
:Code: 409 Conflict

Unlink Bucket
=============

Unlink a bucket from a specified user. Primarily useful for changing
bucket ownership.

Syntax
~~~~~~

::

	POST /{admin}/bucket?format=json HTTP/1.1
	Host {fqdn}


Request Parameters
~~~~~~~~~~~~~~~~~~

``bucket``

:Description: The bucket to unlink.
:Type: String
:Example: ``foo_bucket``
:Required: Yes

``uid``

:Description: The user ID to unlink the bucket from.
:Type: String
:Example: ``foo_user``
:Required: Yes

Response Entities
~~~~~~~~~~~~~~~~~

None.

Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~

``BucketUnlinkFailed``

:Description: Unable to unlink bucket from specified user.
:Code: 409 Conflict

Link Bucket
===========

Link a bucket to a specified user, unlinking the bucket from
any previous user.

Syntax
~~~~~~

::

	PUT /{admin}/bucket?format=json HTTP/1.1
	Host {fqdn}


Request Parameters
~~~~~~~~~~~~~~~~~~

``bucket``

:Description: The bucket to unlink.
:Type: String
:Example: ``foo_bucket``
:Required: Yes

``uid``

:Description: The user ID to link the bucket to.
:Type: String
:Example: ``foo_user``
:Required: Yes

Response Entities
~~~~~~~~~~~~~~~~~

``bucket``

:Description: Container for single bucket information.
:Type: Container

``name``

:Description: The name of the bucket.
:Type: String
:Parent: ``bucket``

``pool``

:Description: The pool the bucket is stored in.
:Type: String
:Parent: ``bucket``

``id``

:Description: The unique bucket id.
:Type: String
:Parent: ``bucket``

``marker``

:Description: Internal bucket tag.
:Type: String
:Parent: ``bucket``

``owner``

:Description: The user id of the bucket owner.
:Type: String
:Parent: ``bucket``

``usage``

:Description: Storage usage information.
:Type: Container
:Parent: ``bucket``

``index``

:Description: Status of bucket index.
:Type: String
:Parent: ``bucket``

Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~

``BucketUnlinkFailed``

:Description: Unable to unlink bucket from specified user.
:Code: 409 Conflict

``BucketLinkFailed``

:Description: Unable to link bucket to specified user.
:Code: 409 Conflict

Get Object
==========

Get an existing object. NOTE: Does not require owner to be non-suspended.

Syntax
~~~~~~

::

	GET /{admin}/bucket?object?format=json HTTP/1.1
	Host {fqdn}

Request Parameters
~~~~~~~~~~~~~~~~~~

``bucket``

:Description: The bucket containing the object to be retrieved.
:Type: String
:Example: ``foo_bucket``
:Required: Yes

``object``

:Description: The object to be retrieved.
:Type: String
:Example: ``foo.txt``
:Required: Yes

Response Entities
~~~~~~~~~~~~~~~~~

If successful, returns the desired object.

``object``

:Description: The desired object.
:Type: Object

Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~

``NoSuchObject``

:Description: Specified object does not exist.
:Code: 404 Not Found

Head Object
===========

Verify the existence of an object. If the object exists,
metadata headers for the object will be returned.

Syntax
~~~~~~

::

	HEAD /{admin}/bucket?object HTTP/1.1
	Host {fqdn}

Request Parameters
~~~~~~~~~~~~~~~~~~

``bucket``

:Description: The bucket containing the object to be retrieved.
:Type: String
:Example: ``foo_bucket``
:Required: Yes

``object``

:Description: The object to be retrieved.
:Type: String
:Example: ``foo.txt``
:Required: Yes

Response Entities
~~~~~~~~~~~~~~~~~

None.

Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~

``NoSuchObject``

:Description: Specified object does not exist.
:Code: 404 Not Found

Remove Object
=============

Remove an existing object. NOTE: Does not require owner to be non-suspended.

Syntax
~~~~~~

::

	DELETE /{admin}/bucket?object?format=json HTTP/1.1
	Host {fqdn}

Request Parameters
~~~~~~~~~~~~~~~~~~

``bucket``

:Description: The bucket containing the object to be removed.
:Type: String
:Example: ``foo_bucket``
:Required: Yes

``object``

:Description: The object to remove.
:Type: String
:Example: ``foo.txt``
:Required: Yes

Response Entities
~~~~~~~~~~~~~~~~~

None.

Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~

``NoSuchObject``

:Description: Specified object does not exist.
:Code: 404 Not Found

``ObjectRemovalFailed``

:Description: Unable to remove objects.
:Code: 409 Conflict

Get Cluster Info
================

Get cluster information.

Syntax
~~~~~~

::

	GET /{admin}/cluster?format=json HTTP/1.1
	Host {fqdn}


Response Entities
~~~~~~~~~~~~~~~~~

If successful, returns cluster pool configuration.

``cluster``

:Description: Contains current cluster pool configuration.
:Type: Container

``domain_root``

:Description: root of all buckets.
:Type: String
:Parent: ``cluster``

``control_pool``

:Description: 
:Type: String
:Parent: ``cluster``

``gc_pool``

:Description: Garbage collection pool.
:Type: String
:Parent: ``cluster``

``log_pool``

:Description: Log pool.
:Type: String
:Parent: ``cluster``

``intent_log_pool``

:Description: Intent log pool.
:Type: String
:Parent: ``cluster``

``usage_log_pool``

:Description: Usage log pool.
:Type: String
:Parent: ``cluster``

``user_keys_pool``

:Description: User key pool.
:Type: String
:Parent: ``cluster``

``user_email_pool``

:Description: User email pool.
:Type: String
:Parent: ``cluster``

``user_swift_pool``

:Description: Pool of swift users.
:Type: String
:Parent: ``cluster``

Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~

TBD.

Example Response
~~~~~~~~~~~~~~~~

::

	HTTP/1.1 200
	Content-Type: application/json

	{
	  "domain_root": ".rgw",
	  "control_pool": ".rgw.control",
	  "gc_pool": ".rgw.gc",
	  "log_pool": ".log",
	  "intent_log_pool": ".intent-log",
	  "usage_log_pool": ".usage",
	  "user_keys_pool": ".users",
	  "user_email_pool": ".users.email",
	  "user_swift_pool": ".users.swift",
	  "user_uid_pool ": ".users.uid"
	}



Add Placement Pool
==================

Make a pool available for data placement.

Syntax
~~~~~~

::

	PUT /{admin}/pool?format=json HTTP/1.1
	Host {fqdn}


Request Parameters
~~~~~~~~~~~~~~~~~~

``pool``

:Description: The pool to be made available for data placement.
:Type: String
:Example: ``foo_pool``
:Required: Yes

``create``

:Description: Creates the data pool if it does not exist.
:Type: Boolean
:Example: False [False]
:Required: No

Response Entities
~~~~~~~~~~~~~~~~~

TBD.

Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~

TBD.

Remove Placement Pool
=====================

Make a pool unavailable for data placement.

Syntax
~~~~~~

::

	DELETE /{admin}/pool?format=json HTTP/1.1
	Host {fqdn}


Request Parameters
~~~~~~~~~~~~~~~~~~

``pool``

:Description: The existing pool to be made available for data placement.
:Type: String
:Example: ``foo_pool``
:Required: Yes

``destroy``

:Description: Destroys the pool after removing it from the active set.
:Type: Boolean
:Example: False [False]
:Required: No

Response Entities
~~~~~~~~~~~~~~~~~

TBD.

Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~

TBD.

List Available Data Placement Pools
===================================

List current pools available for data placement.

Syntax
~~~~~~

::

	GET /{admin}/pool?format=json HTTP/1.1
	Host {fqdn}


Response Entities
~~~~~~~~~~~~~~~~~

If successful, returns a list of pools available for data placement.

``pools``

:Description: Contains currently available pools for data placement.
:Type: Container

Get Bucket or Object Policy
===========================

Read the policy of an object or bucket.

Syntax
~~~~~~

::

	GET /{admin}/bucket?policy?format=json HTTP/1.1
	Host {fqdn}


Request Parameters
~~~~~~~~~~~~~~~~~~

``bucket``

:Description: The bucket to read the policy from.
:Type: String
:Example: ``foo_bucket``
:Required: Yes

``object``

:Description: The object to read the policy from.
:Type: String
:Example: ``foo.txt``
:Required: No

Response Entities
~~~~~~~~~~~~~~~~~

If successful, returns the object or bucket policy

``policy``

:Description: Access control policy.
:Type: Container

Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~

``IncompleteBody``

:Description: Either bucket was not specified for a bucket policy request or bucket 
              and object were not specified for an object policy request.
:Code: 400 Bad Request

Add A User Capability
=====================

Add an administrative capability to a specified user.

Syntax
~~~~~~

::

	PUT /{admin}/user?caps?format=json HTTP/1.1
	Host {fqdn}

Request Parameters
~~~~~~~~~~~~~~~~~~

``uid``

:Description: The user ID to add an administrative capability to.
:Type: String
:Example: ``foo_user``
:Required: Yes

``caps``

:Description: The administrative capability to add to the user.
:Type: String
:Example: ``usage=read, write``
:Required: Yes

Response Entities
~~~~~~~~~~~~~~~~~

If successful, the response contains the user's capabilities.

``user``

:Description: A container for the user data information.
:Type: Container
:Parent: ``user``

``user_id``

:Description: The user id.
:Type: String
:Parent: ``user``

``caps``

:Description: User capabilities.
:Type: Container
:Parent: ``user``


Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~

``InvalidCap``

:Description: Attempt to grant invalid admin capability.
:Code: 400 Bad Request

Example Request
~~~~~~~~~~~~~~~

::

	PUT /{admin}/user?caps?format=json HTTP/1.1
	Host: {fqdn}
	Content-Type: text/plain
	Authorization: {your-authorization-token}

	usage=read


Remove A User Capability
========================

Remove an administrative capability from a specified user.

Syntax
~~~~~~

::

	DELETE /{admin}/user?caps?format=json HTTP/1.1
	Host {fqdn}

Request Parameters
~~~~~~~~~~~~~~~~~~

``uid``

:Description: The user ID to remove an administrative capability from.
:Type: String
:Example: ``foo_user``
:Required: Yes

``caps``

:Description: The administrative capabilities to remove from the user.
:Type: String
:Example: ``usage=read, write``
:Required: Yes

Response Entities
~~~~~~~~~~~~~~~~~

If successful, the response contains the user's capabilities.

``user``

:Description: A container for the user data information.
:Type: Container
:Parent: ``user``

``user_id``

:Description: The user id.
:Type: String
:Parent: ``user``

``caps``

:Description: User capabilities.
:Type: Container
:Parent: ``user``


Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~

``InvalidCap``

:Description: Attempt to remove an invalid admin capability.
:Code: 400 Bad Request

``NoSuchCap``

:Description: User does not possess specified capability.
:Code: 404 Not Found

List Expired Garbage Collection Items
=====================================

List objects scheduled for garbage collection.

Syntax
~~~~~~

::

	GET /{admin}/garbage?format=json HTTP/1.1
	Host {fqdn}

Request Parameters
~~~~~~~~~~~~~~~~~~

None.

Response Entities
~~~~~~~~~~~~~~~~~

If expired garbage collection items exist, a list of such objects
will be returned.

``garbage``

:Description: Expired garbage collection items.
:Type: Container

``object``

:Description: A container garbage collection object information.
:Type: Container
:Parent: ``garbage``

``name``

:Description: The name of the object.
:Type: String
:Parent: ``object``

``expired``

:Description: The date at which the object expired.
:Type: String
:Parent: ``object``

Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~

TBD.

Manually Processes Garbage Collection Items
===========================================

List objects scheduled for garbage collection.

Syntax
~~~~~~

::

	DELETE /{admin}/garbage?format=json HTTP/1.1
	Host {fqdn}

Request Parameters
~~~~~~~~~~~~~~~~~~

None.

Response Entities
~~~~~~~~~~~~~~~~~

If expired garbage collection items exist, a list of removed objects
will be returned.

``garbage``

:Description: Expired garbage collection items.
:Type: Container

``object``

:Description: A container garbage collection object information.
:Type: Container
:Parent: ``garbage``

``name``

:Description: The name of the object.
:Type: String
:Parent: ``object``

``expired``

:Description: The date at which the object expired.
:Type: String
:Parent: ``object``

Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~

TBD.

Show Log Objects
================

Show log objects

Syntax
~~~~~~

::

	GET /{admin}/log?format=json HTTP/1.1
	Host {fqdn}

Request Parameters
~~~~~~~~~~~~~~~~~~

``object``

:Description: The log object to return.
:Type: String:
:Example: ``2012-10-11-09-4165.2-foo_bucket``
:Required: No

Response Entities
~~~~~~~~~~~~~~~~~

If no object is specified, returns the full list of log objects.

``log-objects``

:Description: A list of log objects.
:Type: Container

``object``

:Description: The name of the log object.
:Type: String

``log``

:Description: The contents of the log object.
:Type: Container

Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~

None.

Standard Error Responses
========================

``AccessDenied``

:Description: Access denied.
:Code: 403 Forbidden

``InternalError``

:Description: Internal server error.
:Code: 500 Internal Server Error

``NoSuchUser``

:Description: User does not exist.
:Code: 404 Not Found

``NoSuchBucket``

:Description: Bucket does not exist.
:Code: 404 Not Found

``NoSuchKey``

:Description: No such access key.
:Code: 404 Not Found
