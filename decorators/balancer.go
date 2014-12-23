package metafora

import (
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/araddon/qlbridge/builtins"
	"github.com/araddon/qlbridge/vm"
	"github.com/lytics/metafora"
)

func init() {
	builtins.LoadAllBuiltins()
}

//TODO Write an ETCD implementation for:
//   1 TaskClient - A metafora client that writes a `selector` to etcd during while
//                 creating the task
//	 2 EtcdMetaData - implements the etcd Metadata client used by the Selectable Balancer
//                 to look up Metadata info by taskspace
//   3 Etcd Main() Example that shows adding labels for the nodeid to etcd, so the
//                 MetadataClient for etcd can read them out.

/*
	Selectors are associated with tasks
*/
type Selector string

func (s Selector) Matches(l url.Values, hd url.Values) (bool, error) {
	vals := make(url.Values)
	for k, v := range hd {
		vals[k] = v
	}
	for k, v := range l { // labels can override hostdata.
		vals[k] = v
	}

	writeContext := vm.NewContextSimple()
	readContext := vm.NewContextUrlValues(vals)

	exprVm, err := vm.NewVm(string(s))
	if err != nil {
		return false, err
	}

	err = exprVm.Execute(writeContext, readContext)
	if err != nil {
		return false, err
	}

	val, ok := writeContext.Get("")

	if ok {
		switch v := val.Value().(type) {
		case bool:
			return v, nil // happy path
		default:
			return false, fmt.Errorf("metafora: selector: unknown type from write context")
		}
	} else {
		return false, fmt.Errorf("metafora: selector: failed to get value from write context")
	}

	return false, nil
}

const HostName = "host_name"

func hostdata() url.Values {
	d := make(url.Values)
	host, _ := os.Hostname()
	d.Set(HostName, host)
	return d
}

type MetadataClient interface {
	Selector(taskid string) Selector
	Labels(nodeid string) url.Values
}

//Balancer logic below here:
func WrapBalancerAsSelectable(nodeid string, baseBalancer metafora.Balancer, cs metafora.ClusterState, md MetadataClient) metafora.Balancer {
	return &SelectBalancer{
		nodeid: nodeid,
		cs:     cs,
		md:     md,
		bc:     nil,
		base:   baseBalancer,
	}
}

//Add the selectable layer to an existing balancer strategy
type SelectBalancer struct {
	nodeid string
	cs     metafora.ClusterState
	md     MetadataClient
	bc     metafora.BalancerContext
	base   metafora.Balancer // Decorator pattern,
}

func (b *SelectBalancer) Init(s metafora.BalancerContext) {
	b.bc = s
	b.base.Init(s)
}

func (b *SelectBalancer) CanClaim(taskid string) bool {
	defer func() {
		if r := recover(); r != nil {
			b.bc.Log(metafora.LogLevelError, "metafora: selector: panic'ed taskid:%v panic_msg:%v", taskid, r)
		}
	}()

	selector := b.md.Selector(taskid)
	labels := b.md.Labels(b.nodeid)
	hostdata := hostdata()

	if selector != "" { //If no selectors defined for task, then just use the base balancer
		matches, err := selector.Matches(labels, hostdata)
		if err != nil {
			b.bc.Log(metafora.LogLevelWarn, "Error processing selectors: taskid:%v labels:[%v] selectors:[%v] error:%v", taskid, labels, selector, err)
			return false
		}
		if !matches {
			return false
		}
	}

	//If all conditions are met, then do fair balancing
	// if this was one of our released tasks then delay claiming it, vs rejecting it
	// We maybe the only node that can reclaim the task due to selectors/labels.
	if !b.base.CanClaim(taskid) {
		time.Sleep(200 * time.Millisecond)
		return true
	}

	return true
}

// Balance releases tasks based on what the base balancer decides
func (b *SelectBalancer) Balance() []string {
	return b.base.Balance()
}
