"""Contains functiosn for updating neuprint.

Handles merge and split events.  Note: mutations should only be performed
on non-versioned databases.

"""

import neuprint as neu
import json

class NeuPrintUpdater:
    """Manages updates to a given dataset and server.
    """

    def __init__(self, server, dataset, neuron_pre=2, neuron_post=10, verify=True):
        self.client = neu.Client(server, verify=verify)
        self.dataset = dataset
        
        # criteria for Segment to be a Neuron
        self.neuron_pre = neuron_pre
        self.neuron_post = neuron_post

    def merge_neurons(self, bodylist, properties=None, uuid=None, timestamp=None, debug=False):
        """Merge list of neurons together.
        
        Note: an error will result in an exception.  None of the merge will have been performed.

        Args:
            bodylist (list): list of body ids, first body id is the resulting body id
            properties (dict): custom properties to overwrite current properties (optional)
            uuid (str): UUID where modification occurred
            timestamp (int): number of seconds, unix time when mutation occured
            debug (boolean): if true, the transaction is not actually saved
        """

        if len(bodylist) < 2:
            raise RuntimeError("require at least 2 bodies to be merged")

        bodyset = set(bodylist)
        self.client.start_transaction(self.dataset)

        try:
            # grab node information from both and manually concatenate, then patch with properties
            query_nodeinfo = f"MATCH (n :`{self.dataset}_Segment`) WHERE n.bodyId in {bodylist} return n AS nprop, id(n) AS nid"
            info_df = self.client.query_transaction(query_nodeinfo)
            raise RuntimeError("test error:")

            body2id = {}
            infoarray = []
            # make sure body 1 is first in the info list
            for idx, row in info_df.iterrows():
                bid = row["nprop"]["bodyId"]
                nid = row["nid"]
                info = row["nprop"]
                body2id[bid] = nid
                if bid == bodylist[0]:
                    tmp = [info]
                    tmp.extend(infoarray)
                    infoarray = tmp
                else:
                    infoarray.append(info)
            idset = set(body2id.values())

            if len(body2id) != len(bodylist):
                raise RuntimeError("cannot find all the provided bodies in the database")

            # combine all properties, including roi info and add specified properties
            mergedinfo = combine_properties(infoarray, ["pre", "post", "size"])
                
            # default provided properties
            if properties is not None:
                mergedinfo.update(properties)

            if "cropped" in mergedinfo and "cropped" not in infoarray[0]:
                del mergedinfo["cropped"]
                
            # get connection info
            queryconn = f"MATCH (n :`{self.dataset}_Segment`)-[x :ConnectsTo]-(m) WHERE n.bodyId in {bodylist} RETURN x AS conn, id(m) AS other_nodeid, id(startnode(x)) as head"
            body_connections = self.client.query_transaction(queryconn)

            # parse conflicts and accumulate new connection ROI
            new_outputs = {}
            new_inputs = {}
            new_autapse = None

            for idx, row in body_connections.iterrows():
                if row["other_nodeid"] in idset:
                    if autapse_data is None:
                        new_autapse = row["conn"]
                    else:
                        new_autapse = combine_properties([autapse_data, row["conn"]], ["weight", "weightHP"])
                else:
                    if row["head"] in idset:
                        # is output
                        if row["other_nodeid"] not in new_outputs:
                            new_outputs[row["other_nodeid"]] = row["conn"]
                        else:
                            new_outputs[row["other_nodeid"]] = combine_properties([new_outputs[row["other_nodeid"]], row["conn"]], ["weight", "weightHP"])
                    else:
                        if row["other_nodeid"] not in new_inputs:
                            new_inputs[row["other_nodeid"]] = row["conn"]
                        else:
                            new_inputs[row["other_nodeid"]] = combine_properties([new_inputs[row["other_nodeid"]], row["conn"]], ["weight", "weightHP"])

            # grab synapse set ids from conflict list
            # also add internal connections as a special case
            conf_outputs = set(new_outputs.keys()).union(idset)
            conf_inputs = set(new_inputs.keys()).union(idset)

            ss_query = f"MATCH (n :`{self.dataset}_Segment`)-[:Contains]->(x)-[:ConnectsTo]->(x2)<-[:Contains]-(m) WHERE id(n) in {list(idset)} AND id(m) in {list(conf_outputs)} RETURN true AS isout, id(m) as id2, id(x) as ss_id, id(x2) as ss_id2 UNION MATCH (n :`{self.dataset}_Segment`)-[:Contains]->(x)<-[:ConnectsTo]-(x2)<-[:Contains]-(m) WHERE id(n) in {list(idset)} AND id(m) in {list(conf_inputs)} RETURN false as isout, id(m) as id2, id(x) as ss_id, id(x2) as ss_id2"
            ss_list_df = self.client.query_transaction(ss_query)

            # find synapse sets to collapse
            # (target: [(base id, base id2), ... collapse pairs]

            ss_groups = {}
            for idx, row in ss_list_df.iterrows():
                targetid = row["id2"]
                
                # autapse goes to unused id
                if row["id2"] in idset:
                    targetid = 9999999999999999999
                elif row["isout"]: # output and input should get separate index
                    targetid = targetid * -1
                    
                if targetid not in ss_groups:
                    ss_groups[targetid] = []
                ss_groups[targetid].append((row["ss_id"], row["ss_id2"]))

            
            # 1. merge all nodes

            merge_query = f"MATCH (n) WHERE id(n) in {list(idset)} WITH collect(n) AS nlist call apoc.refactor.mergeNodes(nlist,{{properties:\"discard\", mergeRels:true}}) yield node return id(node) as id"
            merge_df = self.client.query_transaction(merge_query)
            if len(merge_df) != 1:
                self.client.kill_transaction()
                raise RuntimeError("node was not proeprly created")

            # grab result id from result
            baseid = merge_df.iloc[0][0]

            # 2. set connection sets
            # set new connection roi info from list (including autapse)
            input_props =[]
            output_props = []
            for nodeid, prop in new_inputs.items():
                input_props.append({"nid": nodeid, "props": prop})
            for nodeid, prop in new_outputs.items():
                output_props.append({"nid": nodeid, "props": prop})
            if new_autapse is not None:
                output_props.append({"nid": baseid, "props": new_autapse})

            input_propstr = create_propstr(input_props)
            output_propstr = create_propstr(output_props)

            if len(input_props) > 0:
                update_in_query = f"UNWIND {input_propstr} AS data MATCH (n)<-[x :ConnectsTo]-(m) WHERE id(n) = {baseid} AND id(m) = data.nid SET x = data.props"
                self.client.query_transaction(update_in_query)
            if len(output_props) > 0:
                update_out_query = f"UNWIND {output_propstr} AS data MATCH (n)-[x :ConnectsTo]->(m) WHERE id(n) = {baseid} AND id(m) = data.nid SET x = data.props"
                self.client.query_transaction(update_out_query)

            # 3. merge synapse sets

            merge_list = []
            for key, ss_list in ss_groups.items():
                ss1s = []
                ss2s = []
                for (item1, item2) in ss_list:
                    ss1s.append(item1)
                    ss2s.append(item2)
                merge_list.append(ss1s)
                merge_list.append(ss2s)

            merge_query = f"UNWIND {merge_list} AS data MATCH (n) WHERE id(n) in data WITH COLLECT(n) AS nlist, data CALL apoc.refactor.mergeNodes(nlist,{{properties:\"discard\", mergeRels:true}}) yield node return id(node) as id"
            self.client.query_transaction(merge_query)

            # 4. write node properties and meta

            # threshold for Segment to Neuron
            labelstr = ""
            if mergedinfo["pre"] >= self.neuron_pre or mergedinfo["post"] >= self.neuron_post:
                labelstr = f", n:`{self.dataset}_Neuron`, n:Neuron"

            # format string properly
            mergedinfo_str = format_prop(mergedinfo)

            # set node props
            nu_query = f"MATCH (n) WHERE id(n) = {baseid} SET n = {mergedinfo_str} {labelstr}"
            self.client.query_transaction(nu_query)

            uuidstr = ""
            if uuid is not None:
                uuidstr = f", m.uuid = {uuid}"

            # !! set meta time stamp (other stats shouldn't change because no ROI change)
            if timestamp is not None:
                update_time = f"MATCH (m :`{self.dataset}_Meta`) SET m.lastDatabaseEdit = datetime({{ epochSeconds: {timestamp} }}) {uuidstr}"
            else:
                update_time = f"MATCH (m :`{self.dataset}_Meta`) SET m.lastDatabaseEdit = datetime() {uuidstr}"
            self.client.query_transaction(update_time)
        except:
            try:
                self.client.kill_transaction()
                raise
            except:
                pass
            raise

        # don't save merge if in debug mode 
        if debug:
            self.client.kill_transaction()
        else:
            self.client.commit_transaction()


####### helper functions #############

# conditional fetch from dictionary
def dfetch(container, key, default=0):
    if key not in container:
        return default
    return container[key]

# roiinfo merge with map
def merge_roiInfoMap(infoarray):
    currinfo = {}
    for info in infoarray:
        if "roiInfo" not in info:
            continue
  
        for roi, val in info["roiInfo"].items():
            if roi in currinfo:
                currinfo[roi]["pre"] = dfetch(currinfo[roi], "pre") + dfetch(val, "pre")
                currinfo[roi]["post"] = dfetch(currinfo[roi], "post") + dfetch(val, "post")
            else:
                currinfo[roi] = val
    return currinfo

# roiInfo merge with string
def merge_roiInfo(infoarray):
    currinfo = {}
    for info in infoarray:
        if "roiInfo" not in info:
            continue
            
        tmproi = json.loads(info["roiInfo"])  
        for roi, val in tmproi.items():
            if roi in currinfo:
                currinfo[roi]["pre"] = dfetch(currinfo[roi], "pre") + dfetch(val, "pre")
                currinfo[roi]["post"] = dfetch(currinfo[roi], "post") + dfetch(val, "post")
            else:
                currinfo[roi] = val
    return json.dumps(currinfo)

# return new property where supplied weights are added and "roiInfo is merged"
# (first item is the default)
def combine_properties(infoarray, addedprops=[]):
    # merge all rows in for loop
    mergedinfo = infoarray[0].copy()
    for iter1 in range(len(infoarray)-2, -1, -1):
        mergedinfo.update(infoarray[iter1])
    
    for prop in addedprops:
        tmpinfo = dfetch(infoarray[0], prop)
        for info in infoarray[1:]:
            tmpinfo += dfetch(info, prop)
        mergedinfo[prop] = tmpinfo
    
    mergedinfo["roiInfo"] = merge_roiInfo(infoarray)
    return mergedinfo

def format_prop(prop):
    keys = prop.keys()
    prop_str = json.dumps(prop)
    for key in keys:
        prop_str = prop_str.replace("\""+key+"\"", "`"+key+"`")
    return prop_str

# create cypher prop string from array of properties
def create_propstr(prop_arr):
    keys = set()
    for prop in prop_arr:
        keys = keys.union(set(prop.keys()))
        if "props" in prop:
            keys = keys.union(set(prop["props"].keys()))
    propstr = json.dumps(prop_arr)

    for key in keys:
        propstr = propstr.replace("\""+key+"\"", key)
    return propstr


