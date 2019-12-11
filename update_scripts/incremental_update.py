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

    def merge_segments(self, bodylist, properties=None, uuid=None, timestamp=None, debug=False):
        """Merge list of segments together.
        
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
                    if new_autapse is None:
                        new_autapse = row["conn"]
                    else:
                        new_autapse = combine_properties([new_autapse, row["conn"]], ["weight", "weightHP"])
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

            ss_query = f"MATCH (n :`{self.dataset}_Segment`)-[:Contains]->(x)-[:ConnectsTo]->(x2)<-[:Contains]-(m) WHERE id(n) in {list(idset)} AND id(m) in {list(conf_outputs)} RETURN true AS isout, id(n) as id1, id(m) as id2, id(x) as ss_id, id(x2) as ss_id2 UNION MATCH (n :`{self.dataset}_Segment`)-[:Contains]->(x)<-[:ConnectsTo]-(x2)<-[:Contains]-(m) WHERE id(n) in {list(idset)} AND id(m) in {list(conf_inputs)} RETURN false as isout, id(n) as id1, id(m) as id2, id(x) as ss_id, id(x2) as ss_id2"
            ss_list_df = self.client.query_transaction(ss_query)

            # find synapse sets to collapse
            # (target: [(base id, base id2), ... collapse pairs]

            ss_groups = {}
            for idx, row in ss_list_df.iterrows():
                targetid = row["id2"]
                
                # autapse goes to unused id
                if row["id2"] in idset:
                    if not row["isout"]:
                        continue # only consider one direction
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
                input_props.append({"nid": int(nodeid), "props": prop})
            for nodeid, prop in new_outputs.items():
                output_props.append({"nid": int(nodeid), "props": prop})
            if new_autapse is not None:
                output_props.append({"nid": int(baseid), "props": new_autapse})

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
                if len(ss_list) > 1:
                    for (item1, item2) in ss_list:
                        ss1s.append(item1)
                        ss2s.append(item2)
                    merge_list.append(ss1s)
                    merge_list.append(ss2s)
  
            # only merge ss if there are ss to merge
            if len(merge_list) > 0:
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

            # set meta time stamp (other stats shouldn't change because no ROI change)
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


    def split_segment(self, bodyid, synapse_list, properties2,
            properties1=None, uuid=None, timestamp=None, debug=False):
        """Split a segment from a list of synapse points
        
        Note: an error will result in an exception.  None of the split will have been performed.

        Args:
            bodyid (int): body id of segment to split
            synapse_list (list): [(x,y,z), ...]
            properties2 (dict): properties for the newly split neuron (will inherit properties
            from the old neuron otherwise); "bodyId" must be specified
            properties1 (dict): optional properties for original body
            uuid (str): UUID where modification occurred
            timestamp (int): number of seconds, unix time when mutation occured
            debug (boolean): if true, the transaction is not actually saved
        """

        if len(synapse_list) == 0:
            raise RuntimeError("no synapses provided")

        if "bodyId" not in properties2:
            raise RuntimeError("new bodyId is not specified")

        self.client.start_transaction(self.dataset)

        try:
            # get body information

            # grab meta info
            query_meta = f"MATCH (m :{self.dataset}_Meta) RETURN m.postHPThreshold AS thresh, keys(apoc.convert.fromJsonMap(m.roiInfo)) as rois"
            meta_df = self.client.query_transaction(query_meta)
            syn_thres = meta_df.iloc[0][0]
            roiset = set(meta_df.iloc[0][1])

            # grab node info
            query_nodeinfo = f"MATCH (n :{self.dataset}_Segment) WHERE n.bodyId={bodyid} return n AS nprop, id(n) AS nid"
            info_df = self.client.query_transaction(query_nodeinfo)
            currentinfo = info_df.iloc[0][0]
            baseid = info_df.iloc[0][1]

            # grab all relevant synapses
            synapse_locations = synapselist2points(synapse_list)
            synapse_set = set(synapse_list)
            query_synapses = f"MATCH (n :hemibrain_Segment {{bodyId: {bodyid}}})-[:Contains]->(ss1)-[:Contains]->(syn)-[dir :SynapsesTo]-(syn2)<-[:Contains]-(ss2)<-[:Contains]-(m) WHERE syn.location in {synapse_locations} AND (ss1)-[:ConnectsTo]-(ss2) RETURN id(syn) as synid, id(syn2) as synid2, syn as syn, syn2 as syn2, id(ss1) as ss1_id, id(ss2) as ss2_id, id(m) AS targetid, id(startnode(dir))=id(syn) as isout"
            synapses_df = self.client.query_transaction(query_synapses)

            # compute new roi info per connection, store ss with it
            # maintain a list of synapse point edges to be deleted
            # maintain global roi info, pre, and post count
            synapses_del_edges = [] # (nid syn, nid ss)
            subgraph_nid = set()
            subgraph_nid.add(baseid)
            
            numpre = 0
            numpost = 0
            roiInfo_map = {}
            output_targets = {} # nid: {weight, weightHP, roiInfo map}, ssid1, ssid2 -- get rid of ssid if nothing left
            input_targets = {}
            autapse_info = None
            base_ss = None
            synapse_nids = set()
            synapse_pair = set()

            target_nids = set()

            # prevent double tbar could
            tbar_found = set()

            # prevent double psds
            tbar_out = {}
            tbar_in = {}


            # synaspes for different reflexive relationships
            reflex_in_pre = set()
            reflex_in_post = set()
            reflex_out_pre = set()
            reflex_out_post = set()
            reflex_same_pre = set()
            reflex_same_post = set()

            # edges to clone
            all_edges_set = set()

            for idx, row in synapses_df.iterrows():
                synapses_del_edges.append([row["ss1_id"], row["synid"]])
                synapses_del_edges.append([row["ss2_id"], row["synid2"]])
                
                synapse_nids.add(row["synid"])
                synapse_nids.add(row["synid2"])
                
                syn1 = row["syn"]
                syn2 = row["syn2"]
                
                if syn1["type"] == "pre":
                    synapse_pair.add((row["synid"], row["synid2"]))
                else:
                    synapse_pair.add((row["synid2"], row["synid"]))

                if row["targetid"] != baseid:
                    subgraph_nid.add(row["ss1_id"])
                    subgraph_nid.add(row["ss2_id"])
                    subgraph_nid.add(row["targetid"])
                    target_nids.add(row["targetid"])
                    all_edges_set.add((row["ss1_id"], row["ss2_id"]))
                    all_edges_set.add((baseid, row["ss1_id"]))
                    all_edges_set.add((row["targetid"], row["ss2_id"]))
                    all_edges_set.add((row["ss1_id"], row["synid"]))
                    all_edges_set.add((row["ss2_id"], row["synid2"]))

                loc1 = (syn1["location"]["coordinates"][0], syn1["location"]["coordinates"][1], syn1["location"]["coordinates"][2])
                loc2 = (syn2["location"]["coordinates"][0], syn2["location"]["coordinates"][1], syn2["location"]["coordinates"][2])
                typeinfo = {}
                typeinfo["weight"] = 1
                typeinfo["weightHP"] = 0
                typeinfo["roiInfo"] = {}
                
                roi_inter = set(syn1.keys()).intersection(roiset)
                if syn1["type"] == "pre":
                    prev_tbar = True
                    prev_tbar_target = True
                    if loc1 not in tbar_found:
                        prev_tbar = False
                        tbar_found.add(loc1)
                    if row["targetid"] in tbar_out:
                        if loc1 not in tbar_out[row["targetid"]]:
                            prev_tbar_target = False
                            tbar_out[row["targetid"]].add(loc1)    
                    else:
                        prev_tbar_target = False
                        tbar_out[row["targetid"]] = set([loc1])
                    
                    if not prev_tbar:
                        numpre += 1 
                    for roi in roi_inter:
                        typeinfo["roiInfo"][roi] = {}
                        if not prev_tbar_target:
                            typeinfo["roiInfo"][roi]["pre"] = 1
                        typeinfo["roiInfo"][roi]["post"] = 1
                        if roi in roiInfo_map:
                            if "pre" in roiInfo_map[roi]:
                                if not prev_tbar:
                                    roiInfo_map[roi]["pre"] += 1
                            else:
                                roiInfo_map[roi]["pre"] = 1
                        else:
                            roiInfo_map[roi] = {}
                            roiInfo_map[roi]["pre"] = 1
                    if syn2["confidence"] >= syn_thres:
                        typeinfo["weightHP"] = 1
                else:
                    numpost += 1
                    prev_tbar_target = True
                    if row["targetid"] in tbar_in:
                        if loc2 not in tbar_in[row["targetid"]]:
                            prev_tbar_target = False
                            tbar_in[row["targetid"]].add(loc2)    
                    else:
                        prev_tbar_target = False
                        tbar_in[row["targetid"]] = set([loc2])
                        
                    if syn1["confidence"] >= syn_thres:
                        typeinfo["weightHP"] = 1
                    for roi in roi_inter:
                        typeinfo["roiInfo"][roi] = {}
                        typeinfo["roiInfo"][roi]["post"] = 1
                        if not prev_tbar_target:
                            typeinfo["roiInfo"][roi]["pre"] = 1
                        if roi in roiInfo_map:
                            if "post" in roiInfo_map[roi]:
                                roiInfo_map[roi]["post"] += 1
                            else:
                                roiInfo_map[roi]["post"] = 1
                        else:
                            roiInfo_map[roi] = {}
                            roiInfo_map[roi]["post"] = 1        
                typeinfo["roiInfo"] = json.dumps(typeinfo["roiInfo"])
                
                # record ss involved in any autapse
                if row["targetid"] == baseid:
                    base_ss = (row["ss1_id"], row["ss2_id"])
                          
                # check for autapse (eventually subtract this from connection to baseid)
                if loc2 in synapse_set:
                    # only compute for one side of the connection
                    if syn1["type"] == "pre":
                        reflex_same_pre.add(row["synid"])
                        reflex_same_post.add(row["synid2"])
                        if autapse_info is None:
                            autapse_info = typeinfo
                        else:
                            autapse_info = combine_properties([autapse_info, typeinfo], ["weight", "weightHP"])
                else:
                    if row["isout"]:
                        if row["targetid"] == baseid:
                            if syn1["type"] == "pre":
                                reflex_out_pre.add(row["synid"])
                                reflex_out_post.add(row["synid2"])
                            else: # should not execute
                                raise Exception("output is not a pre synapse site")
                            
                        if row["targetid"] not in output_targets:
                            output_targets[row["targetid"]] = (typeinfo, row["ss1_id"], row["ss2_id"])
                        else:
                            prev_info, ig1, ig2 = output_targets[row["targetid"]]
                            output_targets[row["targetid"]] = (combine_properties([prev_info, typeinfo], ["weight", "weightHP"]),
                                                               row["ss1_id"], row["ss2_id"])
                    else:
                        if row["targetid"] == baseid:
                            if syn1["type"] == "post":
                                reflex_in_pre.add(row["synid2"])
                                reflex_in_post.add(row["synid"])
                            else: # should not execute
                                raise Exception("input is not a post synapse site")
                        
                        if row["targetid"] not in input_targets:
                            input_targets[row["targetid"]] = (typeinfo, row["ss1_id"], row["ss2_id"])
                        else:
                            prev_info, ig1, ig2 = input_targets[row["targetid"]]
                            input_targets[row["targetid"]] = (combine_properties([prev_info, typeinfo], ["weight", "weightHP"]),
                                                               row["ss1_id"], row["ss2_id"])

                    
            # compute old and new body info, merge props
            body2info = currentinfo.copy()
            body2info.update(properties2)
            body2info["pre"] = numpre
            body2info["post"] = numpost
            body2info["roiInfo"] = json.dumps(roiInfo_map)

            body1info = currentinfo.copy()
            body1info["pre"] -= numpre
            body1info["post"] -= numpost
            body1info["roiInfo"] = subtract_roiInfo(body1info["roiInfo"], json.dumps(roiInfo_map))


            # query type info explicitly for inputs and outputs
            outs = list(output_targets.keys())
            # just treat autapse like output if it exists
            if autapse_info is not None:
                outs.append(baseid)
            io_query = f"MATCH (n)-[x :ConnectsTo]->(m) WHERE id(n)={baseid} AND id(m) in {outs} RETURN x AS conn, id(m) AS targetid, true AS isoutput UNION MATCH (n)<-[x :ConnectsTo]-(m) WHERE id(n)={baseid} AND id(m) in {list(input_targets.keys())} RETURN x AS conn, id(m) AS targetid, false AS isoutput"
            io_df = self.client.query_transaction(io_query)

            input_conns = {}
            output_conns = {}
            conn_delete_edges = []
            ss_delete = []

            oldautapse = None

            for idx, row in io_df.iterrows():
                # subtract_properties
                
                # treat autapse specially
                if row["targetid"] == baseid:
                    if oldautapse is None:
                        oldautapse = row["conn"]
                else:
                    if row["isoutput"]:
                        newinfo = subtract_properties(row["conn"], output_targets[row["targetid"]][0], ["weight", "weightHP"])
                        # delete edges
                        if newinfo["weight"] == 0:
                            conn_delete_edges.append([baseid, row["targetid"]])
                            ss_delete.append(output_targets[row["targetid"]][1])
                            ss_delete.append(output_targets[row["targetid"]][2])
                        else:
                            output_conns[row["targetid"]] = newinfo
                    else:
                        newinfo = subtract_properties(row["conn"], input_targets[row["targetid"]][0], ["weight", "weightHP"])
                        # delete edges
                        if newinfo["weight"] == 0:
                            conn_delete_edges.append([row["targetid"], baseid])
                            ss_delete.append(input_targets[row["targetid"]][1])
                            ss_delete.append(input_targets[row["targetid"]][2])
                        else:
                            input_conns[row["targetid"]] = newinfo

            # update autapse and set to output
            if oldautapse is not None:
                if baseid in output_targets:
                    oldautapse = subtract_properties(oldautapse, output_targets[baseid][0], ["weight", "weightHP"])
                if baseid in input_targets:
                    oldautapse = subtract_properties(oldautapse, input_targets[baseid][0], ["weight", "weightHP"])
                if autapse_info is not None:
                    oldautapse = subtract_properties(oldautapse, autapse_info, ["weight", "weightHP"])
                if oldautapse["weight"] == 0:
                    conn_delete_edges.append([baseid, baseid])
                    ss_delete.append(base_ss[0])
                    ss_delete.append(base_ss[1])
                else:
                    # just add to output conns (arbitrary)
                    output_conns[baseid] = oldautapse

            # 1. copy network

            # collect all subnetwork nodes
            comp_graph = subgraph_nid.union(synapse_nids)

            standinlist = list(synapse_nids)
            standinlist.extend(list(target_nids))

            all_edges = []
            for edge in all_edges_set:
                all_edges.append([edge[0],edge[1]])

            clone_query = f"MATCH (n) WHERE id(n) in {list(comp_graph)} WITH COLLECT(n) AS nlist MATCH (n) WHERE id(n) in {standinlist} WITH COLLECT([n,n]) AS standlist, nlist UNWIND {all_edges} AS data MATCH (n)-[x]-(m) WHERE id(n)=data[0] AND id(m)=data[1] WITH collect(x) AS elist, standlist, nlist CALL apoc.refactor.cloneSubgraph(nlist, elist, {{skipProperties:[\"bodyId\"], standinNodes:standlist}}) YIELD input, output, error RETURN input AS old, id(output) AS new"
            clone_df = self.client.query_transaction(clone_query)
            newid = int(clone_df[clone_df["old"] == baseid].iloc[0][1])

            """
            # clean up mess from clone

            # delete extra synapse edge between standin node (is this a bug in the cloner?)
            synpair = []
            for (syn1, syn2) in synapse_pair:
                synpair.append([syn1,syn2])
            # delete one synapse edge
            dupsyn_query = f"UNWIND {synpair} AS data MATCH (n)-[x]-(m) WHERE id(n)=data[0] AND id(m)=data[1] WITH n,m,x LIMIT 1 MATCH (n)-[x]-(m) DELETE x"
            self.client.query_transaction(dupsyn_query)

            # delete :ConnectsTo since new ones will be added
            dupconn_query = f"MATCH (n)-[x :ConnectsTo]-(m) WHERE id(n)={newid} DELETE x"
            self.client.query_transaction(dupconn_query)
            """

            # 1b. handle any autapse issues

            # explicitly make relevant SS for autapse or base connection, add synapse links and ss to ss links
            # create synapse sets and relationships
            # segment 1, segment 2
            data = []
            if len(reflex_in_pre) > 0:
                data.append([baseid, newid])
            if len(reflex_out_pre) > 0:
                data.append([newid, baseid])
            if len(reflex_same_pre) > 0:
                data.append([newid, newid])

            if len(data) > 0:
                new_ss_query = f"UNWIND {data} AS data MATCH (n), (m) WHERE id(n) = data[0] AND id(m) = data[1] CREATE (n)-[:Contains]->(a :`{self.dataset}_SynapseSet`:SynapseSet)-[r :ConnectsTo]->(b :`{self.dataset}_SynapseSet`:SynapseSet)<-[:Contains]-(m) RETURN id(n) AS id1, id(m) as id2, id(a) AS ss_pre , id(b) AS ss_post"
                ss_df = self.client.query_transaction(new_ss_query)

                # link synapses to synaspe sets
                data = []
                if len(reflex_in_pre) > 0:
                    res = ss_df[(ss_df["id1"] == baseid) & (ss_df["id2"] == newid)] 
                    pressid = res.iloc[0]["ss_pre"]
                    postssid = res.iloc[0]["ss_post"]
                    for syn in reflex_in_pre:
                        data.append([pressid, syn])
                    for syn in reflex_in_post:
                        data.append([postssid, syn])
                if len(reflex_out_pre) > 0:
                    res = ss_df[(ss_df["id1"] == newid) & (ss_df["id2"] == baseid)] 
                    pressid = res.iloc[0]["ss_pre"]
                    postssid = res.iloc[0]["ss_post"]
                    for syn in reflex_out_pre:
                        data.append([pressid, syn])
                    for syn in reflex_out_post:
                        data.append([postssid, syn])
                if len(reflex_same_pre) > 0:
                    res = ss_df[(ss_df["id1"] == newid) & (ss_df["id2"] == newid)] 
                    pressid = res.iloc[0]["ss_pre"]
                    postssid = res.iloc[0]["ss_post"]
                    for syn in reflex_same_pre:
                        data.append([pressid, syn])
                    for syn in reflex_same_post:
                        data.append([postssid, syn])
                if len(data) > 0:    
                    link_syn_query = f"UNWIND {data} AS data MATCH (n), (m) WHERE id(n) = data[0] AND id(m) = data[1] CREATE (n)-[:Contains]->(m)"
                    self.client.query_transaction(link_syn_query)
            # 2. delete synapse links and connectsto, delete obsolete synapse set nodes and relationships

            links2delete = synapses_del_edges.copy()
            links2delete.extend(conn_delete_edges)
            links2delete.append([newid, newid]) # delete any autapse that is created
            linkdel_query = f"UNWIND {links2delete} AS LINK MATCH (n)-[x]->(m) WHERE id(n)=LINK[0] AND id(m)=LINK[1] DELETE x"
            self.client.query_transaction(linkdel_query)
            
            ssdel_query = f"UNWIND {ss_delete} AS ss MATCH (n) WHERE id(n)=ss DETACH DELETE n"
            self.client.query_transaction(ssdel_query)

            # 3. update old connects to, add new connects to
            # newid should be set to the new node

            # update old connects to
            input_props =[]
            output_props = []
            for nodeid, prop in input_conns.items():
                input_props.append({"nid": int(nodeid), "props": prop})
            for nodeid, prop in output_conns.items():
                output_props.append({"nid": int(nodeid), "props": prop})

            input_propstr = create_propstr(input_props)
            output_propstr = create_propstr(output_props)

            if len(input_props) > 0:
                update_in_query = f"UNWIND {input_propstr} AS data MATCH (n)<-[x :ConnectsTo]-(m) WHERE id(n) = {baseid} AND id(m) = data.nid SET x = data.props"
                self.client.query_transaction(update_in_query)
            if len(output_props) > 0:
                update_out_query = f"UNWIND {output_propstr} AS data MATCH (n)-[x :ConnectsTo]->(m) WHERE id(n) = {baseid} AND id(m) = data.nid SET x = data.props"
                self.client.query_transaction(update_out_query)

            # add new connects to
            input_props =[]
            output_props = []
            for nodeid, prop in input_targets.items():
                input_props.append({"nid": int(nodeid), "props": prop[0]})
            for nodeid, prop in output_targets.items():
                output_props.append({"nid": int(nodeid), "props": prop[0]})

            # add autapse
            if autapse_info is not None:
                output_props.append({"nid": int(newid), "props": autapse_info})
            
            input_propstr = create_propstr(input_props)
            output_propstr = create_propstr(output_props)

            # using create since it will be easier to delete the old edges first
            if len(input_props) > 0:
                add_in_query = f"UNWIND {input_propstr} AS data MATCH (n), (m) WHERE id(n) = {newid} AND id(m) = data.nid CREATE (n)<-[r:ConnectsTo]-(m) SET r = data.props"
                self.client.query_transaction(add_in_query)
            if len(output_props) > 0:
                add_out_query = f"UNWIND {output_propstr} AS data MATCH (n), (m) WHERE id(n) = {newid} AND id(m) = data.nid CREATE (n)-[r:ConnectsTo]->(m) SET r = data.props"
                self.client.query_transaction(add_out_query)

            # 4. write node properties and meta
            is_segment = False
            if body2info["pre"] < self.neuron_pre and body2info["post"] < self.neuron_post:
                is_segment = True
           
            # remove ROI booleans no longer in roi info
            remove_blank_rois(body1info, roiset)
            remove_blank_rois(body2info, roiset)

            # format string properly
            body1info_str = format_prop(body1info)
            body2info_str = format_prop(body2info)

            # set node props
            nu_query = f"MATCH (n) WHERE id(n) = {baseid} SET n = {body1info_str}"
            self.client.query_transaction(nu_query)
            
            if is_segment:
                label_query = f"MATCH (n) WHERE id(n) = {newid} REMOVE n:Neuron:{self.dataset}_Neuron"
                self.client.query_transaction(label_query)
            nu_query = f"MATCH (n) WHERE id(n) = {newid} SET n = {body2info_str}"
            self.client.query_transaction(nu_query)

            # set meta time stamp (other stats shouldn't change because no ROI change)
            uuidstr = ""
            if uuid is not None:
                uuidstr = f", m.uuid = {uuid}"

            # set meta time stamp (other stats shouldn't change because no ROI change)
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


    def update_segment_properties(self, bodyid, properties, uuid=None, timestamp=None, debug=False):
        """Set properties for the given body.
        
        Note: an error will result in an exception.  None of the properties wi set.

        Args:
            bodyid (int): body id 
            properties (dict): custom properties to overwrite current properties
            uuid (str): UUID where modification occurred
            timestamp (int): number of seconds, unix time when mutation occured
            debug (boolean): if true, the transaction is not actually saved
        """

        self.client.start_transaction(self.dataset)

        try:
            query_nodeinfo = f"MATCH (n :`{self.dataset}_Segment` {{bodyId: {bodyid}}}) return n AS nprop, id(n) AS nid"
            info_df = self.client.query_transaction(query_nodeinfo)

            if len(info_df) != 1:
                raise RuntimeError("segment could not be found")
            currprops = info_df.iloc[0][0]
            bid  = info_df.iloc[0][1]

            currprops.update(properties)
            # write node properties and meta

            # format string properly
            properties_str = format_prop(currprops)

            # set node props
            nu_query = f"MATCH (n :`{self.dataset}_Segment`) WHERE id(n) = {bid} SET n = {properties_str}"
            self.client.query_transaction(nu_query)

            uuidstr = ""
            if uuid is not None:
                uuidstr = f", m.uuid = {uuid}"

            # set meta time stamp (other stats shouldn't change because no ROI change)
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

# subtract info2 from info1
def subtract_roiInfo(info1, info2):
    info1 = json.loads(info1)
    info2 = json.loads(info2)
    currinfo = info1.copy()
    for roi, val in info2.items():
        if roi in currinfo:
            currinfo[roi]["pre"] = dfetch(currinfo[roi], "pre") - dfetch(val, "pre")
            currinfo[roi]["post"] = dfetch(currinfo[roi], "post") - dfetch(val, "post")

            if currinfo[roi]["pre"] == 0 and currinfo[roi]["post"] == 0:
                del currinfo[roi] 
        else:
            raise("Not possible")
    return json.dumps(currinfo)

# will mutate calling dict
def remove_blank_rois(info, allrois):
    if "roiInfo" in info:
        rois = set(json.loads(info["roiInfo"]).keys())
        delist = []
        for key in info.keys():
            # any key that is an roi but not in roi info should be deleted
            if key in allrois and key not in rois:
                delist.append(key)
        for key in delist:
            del info[key]

# subtract info2 from info1
def subtract_properties(info1, info2, subprops=[], usemap=False):
    subinfo = info1.copy()
    for prop in subprops:
        subinfo[prop] = dfetch(subinfo, prop) - dfetch(info2, prop)
    
    if "roiInfo" not in info1:
        info1["roiInfo"] = "{}"
    if "roiInfo" not in info2:
        info2["roiInfo"] = "{}"
    subinfo["roiInfo"] = subtract_roiInfo(info1["roiInfo"], info2["roiInfo"])
    return subinfo

# return new property where supplied weights are added and "roiInfo is merged"
# (first item is the default)
def combine_properties(infoarray, addedprops=[], usemap=False):
    # merge all rows in for loop
    mergedinfo = infoarray[0].copy()
    for iter1 in range(len(infoarray)-2, -1, -1):
        mergedinfo.update(infoarray[iter1])
    
    for prop in addedprops:
        tmpinfo = dfetch(infoarray[0], prop)
        for info in infoarray[1:]:
            tmpinfo += dfetch(info, prop)
        mergedinfo[prop] = tmpinfo
    
    if usemap:
        mergedinfo["roiInfo"] = merge_roiInfoMap(infoarray)
    else:
        mergedinfo["roiInfo"] = merge_roiInfo(infoarray)       
    return mergedinfo


def format_prop(prop):
    keys = prop.keys()
    prop_str = json.dumps(prop)
    for key in keys:
        prop_str = prop_str.replace("\""+key+"\"", "`"+key+"`")
    return prop_str

def synapselist2points(synapse_list):
    pointlist = []
    for (x,y,z) in synapse_list:
        pointlist.append(f"point({{x: {x}, y: {y}, z: {z}}})")
    pointliststr = json.dumps(pointlist)
    return pointliststr.replace("\"","")

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


