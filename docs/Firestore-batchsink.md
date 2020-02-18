# Google Cloud Datastore Sink

Description
-----------
This sink writes data to Google Cloud Firestore.

Credentials
-----------
If the plugin is run on a Google Cloud Dataproc cluster, the service account key does not need to be
provided and can be set to `auto-detect`. Credentials will be automatically read from the cluster environment.

If the plugin is not run on a Dataproc cluster, the path to a service account key must be provided.
The service account key can be found on the Dashboard in the Cloud Platform Console.
Make sure the account key has permission to access Google Cloud Firestore.
The service account key file needs to be available on every node in your cluster and
must be readable by all users running the job.

Properties
----------
**Reference Name:** Name used to uniquely identify this sink for lineage, annotating metadata, etc.

**Project ID**: Google Cloud Project ID, which uniquely identifies a project.
It can be found on the Dashboard in the Google Cloud Platform Console.

**Service Account File Path**: Path on the local file system of the service account key used for
authorization. Can be set to `auto-detect` when running on a Dataproc cluster.
When running on other clusters, the file must be present on every node in the cluster.

**Database Id:** Firestore database name. If no value is provided, the `(default)` database will be used.

**Collection Name:** Name of the collection to write the data to.
