
## Prometheus encode API
Following is the supported API format for prometheus encode:

<pre>
 prom:
         metrics: list of prometheus metric definitions, each includes:
                 name: the metric name
                 type: (enum) one of the following:
                     gauge: single numerical value that can arbitrarily go up and down
                     counter: monotonically increasing counter whose value can only increase
                     histogram: counts samples in configurable buckets
                 valuekey: entry key from which to resolve metric value
                 labels: labels to be associated with the metric
                 buckets: histogram buckets
         port: port number to expose "/metrics" endpoint
         prefix: prefix added to each metric name
         expirytime: seconds of no-flow to wait before deleting prometheus data item
</pre>
## Ingest collector API
Following is the supported API format for the netflow collector:

<pre>
 collector:
         hostName: the hostname to listen on
         port: the port number to listen on
</pre>
## Aws ingest API
Following is the supported API format for Aws flow entries:

<pre>
 aws:
         fields: list of aws flow log fields
</pre>
## Transform Generic API
Following is the supported API format for generic transformations:

<pre>
 generic:
         rules: list of transform rules, each includes:
                 input: entry input field
                 output: entry output field
</pre>
## Transform Network API
Following is the supported API format for network transformations:

<pre>
 network:
         rules: list of transform rules, each includes:
                 input: entry input field
                 output: entry output field
                 type: (enum) one of the following:
                     conn_tracking: set output field to value of parameters field only for new flows by matching template in input field
                     add_regex_if: add output field if input field satisfies regex pattern from parameters field
                     add_if: add output field if input field satisfies criteria from parameters field
                     add_subnet: add output subnet field from input field and prefix length from parameters field
                     add_location: add output location fields from input
                     add_service: add output network service field from input port and parameters protocol field
                     add_kubernetes: add output kubernetes fields from input
                 parameters: parameters specific to type
         kubeconfigpath: path to kubeconfig file (optional)
</pre>