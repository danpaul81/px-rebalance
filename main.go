package main

import (
	"context"

	//"encoding/json"
	"flag"
	"fmt"
	"os"

	//"time"

	api "github.com/libopenstorage/openstorage-sdk-clients/sdk/golang"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

type pxvolume struct {
	id      string
	repl    int64
	mounted string
	az      map[string]int
	//replicas []replicasets
}

type replicasets struct {
	node     string
	az       string
	attached bool
}

type pxnode struct {
	id string
	az string
}

var (
	address   = flag.String("address", "127.0.0.1:9100", "Address to server as <address>:<port>")
	pxvolumes []pxvolume
)

func main() {
	ipnodemap := make(map[string]string)
	nodeazmap := make(map[string]string)

	flag.Parse()
	dialOptions := []grpc.DialOption{grpc.WithInsecure()}

	conn, err := grpc.Dial(*address, dialOptions...)
	if err != nil {
		fmt.Printf("Error: %v", err)
		os.Exit(1)
	}

	cluster := api.NewOpenStorageClusterClient(conn)
	// print cluster information
	clusterInfo, err := cluster.InspectCurrent(
		context.Background(),
		&api.SdkClusterInspectCurrentRequest{})
	if err != nil {
		gerr, _ := status.FromError(err)
		fmt.Printf("Error Code[%d] Message[%s]\n",
			gerr.Code(), gerr.Message())
		os.Exit(1)
	}
	fmt.Printf("Connected to Cluster %s (ID %s)\n",
		clusterInfo.GetCluster().GetName(), clusterInfo.GetCluster().GetId())

	// collect / print node information
	nodeclient := api.NewOpenStorageNodeClient(conn)
	nodeEnumResp, err := nodeclient.Enumerate(
		context.Background(),
		&api.SdkNodeEnumerateRequest{})
	if err != nil {
		gerr, _ := status.FromError(err)
		fmt.Printf("Error Code[%d] Message[%s]\n",
			gerr.Code(), gerr.Message())
		os.Exit(1)
	}

	// For each node ID, get its information
	for _, nodeID := range nodeEnumResp.GetNodeIds() {
		node, err := nodeclient.Inspect(
			context.Background(),
			&api.SdkNodeInspectRequest{
				NodeId: nodeID,
			},
		)
		if err != nil {
			gerr, _ := status.FromError(err)
			fmt.Printf("Error Code[%d] Message[%s]\n",
				gerr.Code(), gerr.Message())
			os.Exit(1)
		}
		fmt.Printf("Node ID %s \t SchedulerName %s HostIP %s\n", node.GetNode().GetId(), node.GetNode().GetSchedulerNodeName(), node.GetNode().GetDataIp())
		ipnodemap[node.GetNode().GetDataIp()] = node.GetNode().GetId()

		for key, label := range node.GetNode().GetSchedulerTopology().Labels {

			if key == "topology.portworx.io/zone" {
				nodeazmap[node.GetNode().GetId()] = label
			}
			fmt.Printf("\t %s %s\n", key, label)
		}
		//nd, _ := json.MarshalIndent(node, "", "  ")
		//fmt.Printf(" JSON %s", nd)
	}

	// print / collect volume information
	volumeclient := api.NewOpenStorageVolumeClient(conn)
	volumeEnumResp, err := volumeclient.Enumerate(
		context.Background(),
		&api.SdkVolumeEnumerateRequest{})
	if err != nil {
		gerr, _ := status.FromError(err)
		fmt.Printf("Error Code[%d] Message[%s]\n",
			gerr.Code(), gerr.Message())
		os.Exit(1)
	}

	for _, volumeID := range volumeEnumResp.GetVolumeIds() {
		volume, err := volumeclient.Inspect(
			context.Background(),
			&api.SdkVolumeInspectRequest{
				VolumeId: volumeID,
			},
		)
		if err != nil {
			gerr, _ := status.FromError(err)
			fmt.Printf("Error Code[%d] Message[%s]\n",
				gerr.Code(), gerr.Message())
			os.Exit(1)
		}

		// can a replicaset consist of more than 3 nodes? (e.g. stripesets)
		fmt.Printf("Volume ID %s HA Level %v \n", volume.GetVolume().GetId(), volume.GetVolume().GetSpec().GetHaLevel())
		mounted := ipnodemap[volume.GetVolume().GetAttachedOn()]

		pxvolumes = append(pxvolumes, pxvolume{
			id:      volume.GetVolume().GetId(),
			repl:    volume.GetVolume().GetSpec().GetHaLevel(),
			mounted: mounted,
		})

		pxvolumes[len(pxvolumes)-1].az = make(map[string]int)

		fmt.Printf("\t found %s replicasets \n", volume.GetVolume().GetReplicaSets())

		for _, replicaset := range volume.GetVolume().GetReplicaSets() {
			for _, replicanodes := range replicaset.Nodes {

				if replicanodes == mounted {
					fmt.Printf(" \t Node %s AZ %s *mounted \n", replicanodes, nodeazmap[replicanodes])
				} else {
					fmt.Printf(" \t Node %s AZ %s \n", replicanodes, nodeazmap[replicanodes])
				}

				_, ok := pxvolumes[len(pxvolumes)-1].az[nodeazmap[replicanodes]]
				if ok {
					fmt.Println("inc1")
					pxvolumes[len(pxvolumes)-1].az[nodeazmap[replicanodes]]++
				} else {
					fmt.Println("set1")
					pxvolumes[len(pxvolumes)-1].az[nodeazmap[replicanodes]] = 1
				}
			}
		}

		fmt.Printf("finally pxvolumes %v", pxvolumes)
		//		nd, _ := json.MarshalIndent(volume, "", "  ")
		//		fmt.Printf(" JSON %s \n", nd)
		//for _, volumeID := range
	}
}
