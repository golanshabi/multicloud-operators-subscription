// Copyright 2019 The Kubernetes Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mcmhub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	gerr "github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ghodss/yaml"
	chnv1 "github.com/open-cluster-management/multicloud-operators-channel/pkg/apis/apps/v1"
	chnv1alpha1 "github.com/open-cluster-management/multicloud-operators-channel/pkg/apis/apps/v1"
	dplv1 "github.com/open-cluster-management/multicloud-operators-deployable/pkg/apis/apps/v1"
	dplv1alpha1 "github.com/open-cluster-management/multicloud-operators-deployable/pkg/apis/apps/v1"
	dplutils "github.com/open-cluster-management/multicloud-operators-deployable/pkg/utils"
	releasev1 "github.com/open-cluster-management/multicloud-operators-subscription-release/pkg/apis/apps/v1"
	appv1alpha1 "github.com/open-cluster-management/multicloud-operators-subscription/pkg/apis/apps/v1"
	"github.com/open-cluster-management/multicloud-operators-subscription/pkg/utils"
	subutil "github.com/open-cluster-management/multicloud-operators-subscription/pkg/utils"
	awsutils "github.com/open-cluster-management/multicloud-operators-subscription/pkg/utils/aws"
	manifestWorkV1 "open-cluster-management.io/api/work/v1"
	appv1 "open-cluster-management.io/multicloud-operators-subscription/pkg/apis/apps/v1"
)

const (
	controllerName = "multicluster-operators-hub-subscription"
)

// doMCMHubReconcile process Subscription on hub - distribute it via deployable
func (r *ReconcileSubscription) doMCMHubReconcile(sub *appv1alpha1.Subscription, placementDecisionUpdated bool) ([]ManageClusters, error) {
	substr := fmt.Sprintf("%v/%v", sub.GetNamespace(), sub.GetName())
	klog.V(1).Infof("entry doMCMHubReconcile %v", substr)

	defer klog.V(1).Infof("exit doMCMHubReconcile %v", substr)

	// TO-DO: need to implement the new appsub rolling update with no deployable dependency

	primaryChannel, secondaryChannel, err := r.getChannel(sub)

	if err != nil {
		klog.Errorf("Failed to find a channel for subscription: %s", sub.GetName())

		return nil, err
	}

	if (primaryChannel != nil && secondaryChannel != nil) &&
		(primaryChannel.Spec.Type != secondaryChannel.Spec.Type) {
		klog.Errorf("the type of primary and secondary channels is different. primary channel type: %s, secondary channel type: %s",
			primaryChannel.Spec.Type, secondaryChannel.Spec.Type)

		newError := fmt.Errorf("the type of primary and secondary channels is different. primary channel type: %s, secondary channel type: %s",
			primaryChannel.Spec.Type, secondaryChannel.Spec.Type)

		return nil, newError
	}

	chnAnnotations := primaryChannel.GetAnnotations()

	if chnAnnotations[appv1.AnnotationResourceReconcileLevel] != "" {
		// When channel reconcile rate is changed, this label is used to trigger
		// managed cluster to pick up the channel change and adjust reconcile rate.
		sublabels := sub.GetLabels()

		if sublabels == nil {
			sublabels = make(map[string]string)
		}

		sublabels[appv1.AnnotationResourceReconcileLevel] = chnAnnotations[appv1.AnnotationResourceReconcileLevel]
		klog.Info("Adding subscription label ", appv1.AnnotationResourceReconcileLevel, ": ", chnAnnotations[appv1.AnnotationResourceReconcileLevel])
		sub.SetLabels(sublabels)
	}

	klog.Infof("subscription: %v/%v", sub.GetNamespace(), sub.GetName())

	// Check and add cluster-admin annotation for multi-namepsace application
	//isAdmin := r.AddClusterAdminAnnotation(sub)

	// Add or sync application labels
	r.AddAppLabels(sub)

	var resources []*unstructured.Unstructured

	switch tp := strings.ToLower(string(primaryChannel.Spec.Type)); tp {
	case chnv1alpha1.ChannelTypeGit, chnv1alpha1.ChannelTypeGitHub:
		resources, err = r.GetGitResources(sub)
		//case chnv1alpha1.ChannelTypeHelmRepo:
		//	resources, err = getHelmTopoResources(r.Client, r.cfg, primaryChannel, secondaryChannel, sub, isAdmin)
		//case chnv1alpha1.ChannelTypeObjectBucket:
		//	resources, err = r.getObjectBucketResources(sub, primaryChannel, secondaryChannel, isAdmin)
	}

	manifestTemplate := &manifestWorkV1.ManifestWork{}
	manifestTemplate.Name = "sub-" + sub.Name + "-manifest"
	manifestTemplate.Annotations = sub.Annotations
	manifestTemplate.Kind = "ManifestWork"
	manifestTemplate.APIVersion = "work.open-cluster-management.io/v1"
	manifestTemplate.SetAnnotations(appendToMap(manifestTemplate.GetAnnotations(), "owner-sub",
		sub.GetNamespace()+"/"+sub.GetName()))
	manifestTemplate.SetLabels(appendToMap(manifestTemplate.GetLabels(), "app", sub.GetLabels()["app"]))

	for _, unstructuredResource := range resources {
		var finalResource []byte

		// Add app owner label and sub owner annotation to each resource.

		unstructuredResource.SetAnnotations(appendToMap(unstructuredResource.GetAnnotations(), "owner-sub",
			sub.GetNamespace()+"/"+sub.GetName()))

		unstructuredResource.SetLabels(appendToMap(unstructuredResource.GetLabels(), "app", sub.GetLabels()["app"]))

		if unstructuredResource.GetKind() != "Namespace" && unstructuredResource.GetNamespace() == "" {
			unstructuredResource.SetNamespace(sub.Namespace)
		}

		finalResource, err = json.Marshal(unstructuredResource)
		if err != nil {
			return nil, err
		}

		// Add the object into the manifest
		raw := runtime.RawExtension{Raw: finalResource}
		objToInsert := manifestWorkV1.Manifest{RawExtension: raw}
		manifestTemplate.Spec.Workload.Manifests = append(manifestTemplate.Spec.Workload.Manifests, objToInsert)
	}

	// get all managed clusters.
	clusters, err := r.getClustersByPlacement(sub)

	if err != nil {
		klog.Error("Error in getting clusters:", err)
	}

	// look for the configMap with the old placement
	configMapName := "sub-" + sub.GetName() + "-config"
	var configMap corev1.ConfigMap

	err = r.Client.Get(context.TODO(), client.ObjectKey{Namespace: sub.GetNamespace(), Name: configMapName}, &configMap)
	var deleteManifest manifestWorkV1.ManifestWork

	// if configMap is not found then this is a new subscription so we want to create it.
	if k8serrors.IsNotFound(err) {
		err = r.createConfigMap(sub, &configMap, configMapName, clusters)
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	} else if placementDecisionUpdated {
		// if placementDesicionUpdated is true then the placement was changed so we need to change the propagated clusters.
		oldClusters := strings.Split(configMap.Data["clusters"], " ")

		// delete the manifestWork from the old placement clusters that aren't in the new placement.
		for _, cluster := range difference(oldClusters, clusters) {
			err = r.Client.Get(context.TODO(), client.ObjectKey{Namespace: cluster, Name: manifestTemplate.Name}, &deleteManifest)
			if k8serrors.IsNotFound(err) {
				continue
			} else if err != nil {
				return nil, err
			}

			err = r.Client.Delete(context.TODO(), &deleteManifest)
			if err != nil {
				return nil, err
			}
		}

		var clustersString strings.Builder
		for _, cluster := range clusters[:len(clusters)-1] {
			clustersString.WriteString(cluster.Cluster)
			clustersString.WriteString(" ")
		}

		if len(clusters) != 0 {
			clustersString.WriteString(clusters[len(clusters)-1].Cluster)
		}

		configMap.Data["clusters"] = clustersString.String()

		err = r.Update(context.TODO(), &configMap)
		if err != nil {
			return nil, err
		}
	}

	for _, cluster := range clusters {
		manifestTemplate.SetNamespace(cluster.Cluster)

		//manifestBytes, err := json.Marshal(manifestTemplate)
		//if err != nil {
		//	return nil, err
		//}

		//forceChanges := true
		err = r.Client.Get(context.TODO(), client.ObjectKey{Namespace: cluster.Cluster, Name: manifestTemplate.Name},
			&deleteManifest)
		if k8serrors.IsNotFound(err) {
			manifestTemplate.ResourceVersion = ""
			err = r.Client.Create(context.TODO(), manifestTemplate)
			if err != nil {
				klog.Error(err)
			}
		} else if err != nil {
			klog.Error(err)
		} else {
			manifestTemplate.ResourceVersion = deleteManifest.ResourceVersion
			manifestTemplate.UID = deleteManifest.UID
			err = r.Client.Update(context.TODO(), manifestTemplate)
			if err != nil {
				klog.Error(err)
			}
			//err = r.Client.Patch(context.TODO(), manifestTemplate, client.RawPatch(types.ApplyPatchType, manifestBytes),
			//	&client.PatchOptions{
			//		FieldManager: controllerName,
			//		Force:        &forceChanges,
			//	})
		}
	}

	return clusters, nil
}

// receives the data for a configMap and creates it.
func (r *ReconcileSubscription) createConfigMap(sub *appv1alpha1.Subscription, configMap *corev1.ConfigMap,
	configMapName string, clusters []ManageClusters) error {
	// if configMap not found then create it.
	var clustersString strings.Builder

	configMap.Name = configMapName
	configMap.SetAnnotations(appendToMap(configMap.GetAnnotations(), "owner-sub",
		sub.GetNamespace()+"/"+sub.GetName()))
	configMap.SetLabels(appendToMap(configMap.GetLabels(), "app", sub.GetLabels()["app"]))
	configMap.Namespace = sub.Namespace
	configMap.APIVersion = "v1"
	configMap.Kind = "ConfigMap"
	configMap.Data = map[string]string{}

	if clusters != nil {
		for _, cluster := range clusters[:len(clusters)-1] {
			clustersString.WriteString(cluster.Cluster)
			clustersString.WriteString(" ")
		}

		clustersString.WriteString(clusters[len(clusters)-1].Cluster)
	}

	configMap.Data["clusters"] = clustersString.String()

	err := r.Client.Create(context.TODO(), configMap)
	if err != nil {
		return err
	}

	return nil
}

// appendToMap receive a map, key and value and adds the key - value combination if value is not "".
func appendToMap(mapToAppend map[string]string, key, val string) map[string]string {
	if mapToAppend == nil {
		mapToAppend = map[string]string{}
	}

	if val != "" {
		mapToAppend[key] = val
	}

	return mapToAppend
}

func (r *ReconcileSubscription) AddAppLabels(s *appv1alpha1.Subscription) {
	labels := s.GetLabels()

	if labels == nil {
		labels = make(map[string]string)
	}

	if labels["app"] != "" { // if label "app" exists, sync with "app.kubernetes.io/part-of" label
		if labels["app.kubernetes.io/part-of"] != labels["app"] {
			labels["app.kubernetes.io/part-of"] = labels["app"]
		}
	} else { // if "app" label does not exist, set it and "app.kubernetes.io/part-of" label with the subscription name
		if labels["app.kubernetes.io/part-of"] != s.Name {
			labels["app.kubernetes.io/part-of"] = s.Name
		}

		if labels["app"] != s.Name {
			labels["app"] = s.Name
		}
	}

	s.SetLabels(labels)
}

// if there exists target subscription, update the source subscription based on its target subscription.
// return the target subscription and updated flag
func (r *ReconcileSubscription) updateSubscriptionToTarget(sub *appv1alpha1.Subscription) (*appv1alpha1.Subscription, bool, error) {
	//Fetch target subcription if exists
	annotations := sub.GetAnnotations()

	if annotations == nil || annotations[appv1alpha1.AnnotationRollingUpdateTarget] == "" {
		klog.V(5).Info("Empty annotation or No rolling update target in annotations", annotations)

		err := r.clearSubscriptionTargetDpl(sub)

		return nil, false, err
	}

	targetSub := &appv1alpha1.Subscription{}
	targetSubKey := types.NamespacedName{
		Namespace: sub.Namespace,
		Name:      annotations[appv1alpha1.AnnotationRollingUpdateTarget],
	}
	err := r.Get(context.TODO(), targetSubKey, targetSub)

	if err != nil {
		if apierrors.IsNotFound(err) {
			klog.Infof("target Subscription is gone: %#v.", targetSubKey)

			return nil, false, nil
		}
		// Error reading the object - requeue the request.
		klog.Infof("fetching target Subscription failed: %#v.", err)

		return nil, false, err
	}

	//compare the source subscription with the target subscription. The source subscription placemen is alwayas applied.
	updated := false

	if !reflect.DeepEqual(sub.GetLabels(), targetSub.GetLabels()) {
		klog.V(1).Infof("old label: %#v, new label: %#v", sub.GetLabels(), targetSub.GetLabels())
		sub.SetLabels(targetSub.GetLabels())

		updated = true
	}

	if !reflect.DeepEqual(sub.Spec.Overrides, targetSub.Spec.Overrides) {
		klog.V(1).Infof("old override: %#v, new override: %#v", sub.Spec.Overrides, targetSub.Spec.Overrides)
		sub.Spec.Overrides = targetSub.Spec.Overrides

		updated = true
	}

	if !reflect.DeepEqual(sub.Spec.Channel, targetSub.Spec.Channel) {
		klog.V(1).Infof("old channel: %#v, new channel: %#v", sub.Spec.Channel, targetSub.Spec.Channel)
		sub.Spec.Channel = targetSub.Spec.Channel

		updated = true
	}

	if !reflect.DeepEqual(sub.Spec.Package, targetSub.Spec.Package) {
		klog.V(1).Infof("old Package: %#v, new Package: %#v", sub.Spec.Package, targetSub.Spec.Package)
		sub.Spec.Package = targetSub.Spec.Package

		updated = true
	}

	if !reflect.DeepEqual(sub.Spec.PackageFilter, targetSub.Spec.PackageFilter) {
		klog.V(1).Infof("old PackageFilter: %#v, new PackageFilter: %#v", sub.Spec.PackageFilter, targetSub.Spec.PackageFilter)
		sub.Spec.PackageFilter = targetSub.Spec.PackageFilter

		updated = true
	}

	if !reflect.DeepEqual(sub.Spec.PackageOverrides, targetSub.Spec.PackageOverrides) {
		klog.V(1).Infof("old PackageOverrides: %#v, new PackageOverrides: %#v", sub.Spec.PackageOverrides, targetSub.Spec.PackageOverrides)
		sub.Spec.PackageOverrides = targetSub.Spec.PackageOverrides

		updated = true
	}

	if !reflect.DeepEqual(sub.Spec.TimeWindow, targetSub.Spec.TimeWindow) {
		klog.V(1).Infof("old TimeWindow: %#v, new TimeWindow: %#v", sub.Spec.TimeWindow, targetSub.Spec.TimeWindow)
		sub.Spec.TimeWindow = targetSub.Spec.TimeWindow

		updated = true
	}

	return targetSub, updated, nil
}

//setFoundDplAnnotation set target dpl annotation to the source dpl annoation
func setFoundDplAnnotation(found, dpl, targetDpl *dplv1alpha1.Deployable, updateTargetAnno bool) map[string]string {
	foundanno := found.GetAnnotations()
	if foundanno == nil {
		foundanno = make(map[string]string)
	}

	foundanno[dplv1alpha1.AnnotationIsGenerated] = "true"
	foundanno[dplv1alpha1.AnnotationLocal] = "false"

	if updateTargetAnno {
		if targetDpl != nil {
			foundanno[appv1alpha1.AnnotationRollingUpdateTarget] = targetDpl.GetName()
		} else {
			delete(foundanno, appv1alpha1.AnnotationRollingUpdateTarget)
		}

		subDplAnno := dpl.GetAnnotations()
		if subDplAnno == nil {
			subDplAnno = make(map[string]string)
		}

		if subDplAnno[appv1alpha1.AnnotationRollingUpdateMaxUnavailable] > "" {
			foundanno[appv1alpha1.AnnotationRollingUpdateMaxUnavailable] = subDplAnno[appv1alpha1.AnnotationRollingUpdateMaxUnavailable]
		} else {
			delete(foundanno, appv1alpha1.AnnotationRollingUpdateMaxUnavailable)
		}
	}

	return foundanno
}

//updateDplLabel update found dpl label subacription-pause
func setFoundDplLabel(found *dplv1alpha1.Deployable, sub *appv1alpha1.Subscription) {
	label := found.GetLabels()
	if label == nil {
		label = make(map[string]string)
	}

	labelPause := "false"
	if subutil.GetPauseLabel(sub) {
		labelPause = "true"
	}

	label[dplv1alpha1.LabelSubscriptionPause] = labelPause

	found.SetLabels(label)
}

//setSuscriptionLabel update subscription labels. for now only set subacription-pause label
func setSuscriptionLabel(sub *appv1alpha1.Subscription) {
	label := sub.GetLabels()
	if label == nil {
		label = make(map[string]string)
	}

	labelPause := "false"
	if subutil.GetPauseLabel(sub) {
		labelPause = "true"
	}

	label[dplv1alpha1.LabelSubscriptionPause] = labelPause

	sub.SetLabels(label)
}

//SetTargetDplAnnotation set target dpl annotation to the source dpl annoation
func setTargetDplAnnotation(sub *appv1alpha1.Subscription, dpl, targetDpl *dplv1alpha1.Deployable) map[string]string {
	dplAnno := dpl.GetAnnotations()

	if dplAnno == nil {
		dplAnno = make(map[string]string)
	}

	dplAnno[appv1alpha1.AnnotationRollingUpdateTarget] = targetDpl.GetName()

	subAnno := sub.GetAnnotations()
	if subAnno[appv1alpha1.AnnotationRollingUpdateMaxUnavailable] > "" {
		dplAnno[appv1alpha1.AnnotationRollingUpdateMaxUnavailable] = subAnno[appv1alpha1.AnnotationRollingUpdateMaxUnavailable]
	}

	return dplAnno
}

// checkRollingUpdateAnno check if there needs to update rolling update target annotation to the subscription deployable
func checkRollingUpdateAnno(found, targetDpl, subDpl *dplv1alpha1.Deployable) bool {
	foundanno := found.GetAnnotations()

	if foundanno == nil {
		foundanno = make(map[string]string)
	}

	subDplAnno := subDpl.GetAnnotations()
	if subDplAnno == nil {
		subDplAnno = make(map[string]string)
	}

	if foundanno[appv1alpha1.AnnotationRollingUpdateMaxUnavailable] != subDplAnno[appv1alpha1.AnnotationRollingUpdateMaxUnavailable] {
		return true
	}

	updateTargetAnno := false

	if targetDpl != nil {
		if foundanno[appv1alpha1.AnnotationRollingUpdateTarget] != targetDpl.GetName() {
			updateTargetAnno = true
		}
	} else {
		if foundanno[appv1alpha1.AnnotationRollingUpdateTarget] > "" {
			updateTargetAnno = true
		}
	}

	return updateTargetAnno
}

//GetChannelNamespaceType get the channel namespace and channel type by the given subscription
func (r *ReconcileSubscription) GetChannelNamespaceType(s *appv1alpha1.Subscription) (string, string, string) {
	chNameSpace := ""
	chName := ""
	chType := ""

	if s.Spec.Channel != "" {
		strs := strings.Split(s.Spec.Channel, "/")
		if len(strs) == 2 {
			chNameSpace = strs[0]
			chName = strs[1]
		} else {
			chNameSpace = s.Namespace
		}
	}

	chkey := types.NamespacedName{Name: chName, Namespace: chNameSpace}
	chobj := &chnv1alpha1.Channel{}
	err := r.Get(context.TODO(), chkey, chobj)

	if err == nil {
		chType = string(chobj.Spec.Type)
	}

	return chNameSpace, chName, chType
}

func GetSubscriptionRefChannel(clt client.Client, s *appv1alpha1.Subscription) (*chnv1.Channel, *chnv1.Channel, error) {
	primaryChannel, err := parseGetChannel(clt, s.Spec.Channel)

	if err != nil {
		if apierrors.IsNotFound(err) {
			klog.Errorf("primary channel %s not found for subscription %s/%s", s.Spec.Channel, s.GetNamespace(), s.GetName())

			return nil, nil, err
		}
	}

	secondaryChannel, err := parseGetChannel(clt, s.Spec.SecondaryChannel)

	if err != nil {
		klog.Errorf("secondary channel %s not found for subscription %s/%s", s.Spec.SecondaryChannel, s.GetNamespace(), s.GetName())

		return nil, nil, err
	}

	return primaryChannel, secondaryChannel, err
}

func parseGetChannel(clt client.Client, channelName string) (*chnv1.Channel, error) {
	if channelName == "" {
		return nil, nil
	}

	chNameSpace := ""
	chName := ""
	strs := strings.Split(channelName, "/")

	if len(strs) == 2 {
		chNameSpace = strs[0]
		chName = strs[1]
	}

	chkey := types.NamespacedName{Name: chName, Namespace: chNameSpace}
	channel := &chnv1.Channel{}
	err := clt.Get(context.TODO(), chkey, channel)

	if err != nil {
		return nil, err
	}

	return channel, nil
}

func (r *ReconcileSubscription) getChannel(s *appv1alpha1.Subscription) (*chnv1alpha1.Channel, *chnv1alpha1.Channel, error) {
	return GetSubscriptionRefChannel(r.Client, s)
}

// GetChannelGeneration get the channel generation
func (r *ReconcileSubscription) GetChannelGeneration(s *appv1alpha1.Subscription) (string, error) {
	chNameSpace := ""
	chName := ""

	if s.Spec.Channel != "" {
		strs := strings.Split(s.Spec.Channel, "/")
		if len(strs) == 2 {
			chNameSpace = strs[0]
			chName = strs[1]
		} else {
			chNameSpace = s.Namespace
		}
	}

	chkey := types.NamespacedName{Name: chName, Namespace: chNameSpace}
	chobj := &chnv1alpha1.Channel{}
	err := r.Get(context.TODO(), chkey, chobj)

	if err != nil {
		return "", err
	}

	return strconv.FormatInt(chobj.Generation, 10), nil
}

// UpdateDeployablesAnnotation set all deployables subscribed by the subscription to the apps.open-cluster-management.io/deployables annotation
func (r *ReconcileSubscription) UpdateDeployablesAnnotation(sub *appv1alpha1.Subscription, parentType string) bool {
	orgdplmap := make(map[string]bool)
	organno := sub.GetAnnotations()

	if organno != nil {
		dpls := organno[appv1alpha1.AnnotationDeployables]
		if dpls != "" {
			dplkeys := strings.Split(dpls, ",")
			for _, dplkey := range dplkeys {
				orgdplmap[dplkey] = true
			}
		}
	}

	// map[string]*deployable
	allDpls := r.getSubscriptionDeployables(sub)

	// changes in order of deployables does not mean changes in deployables
	updated := false

	for k := range allDpls {
		if _, ok := orgdplmap[k]; !ok {
			updated = true
			break
		}

		delete(orgdplmap, k)
	}

	if !updated && len(orgdplmap) > 0 {
		updated = true
	}

	if updated {
		dplstr := ""
		for dplkey := range allDpls {
			if dplstr != "" {
				dplstr += ","
			}

			dplstr += dplkey
		}

		klog.Info("subscription updated for ", sub.Namespace, "/", sub.Name, " new deployables:", dplstr)

		subanno := sub.GetAnnotations()
		if subanno == nil {
			subanno = make(map[string]string)
		}

		subanno[appv1alpha1.AnnotationDeployables] = dplstr

		sub.SetAnnotations(subanno)
	}

	// Check and add cluster-admin annotation for multi-namepsace application
	updated = r.AddClusterAdminAnnotation(sub)

	topoFlag := extracResourceListFromDeployables(sub, allDpls, parentType)

	return updated || topoFlag
}

// clearSubscriptionDpls clear the subscription deployable and its rolling update target deployable if exists.
func (r *ReconcileSubscription) clearSubscriptionDpls(sub *appv1alpha1.Subscription) error {
	klog.V(5).Info("No longer hub, deleting sbscription deploayble")

	hubdpl := &dplv1alpha1.Deployable{}
	err := r.Get(context.TODO(), types.NamespacedName{Name: sub.Name + "-deployable", Namespace: sub.Namespace}, hubdpl)

	if err == nil {
		// no longer hub, check owner-reference and delete if it is generated.
		owners := hubdpl.GetOwnerReferences()
		for _, owner := range owners {
			if owner.UID == sub.UID {
				err = r.Delete(context.TODO(), hubdpl)
				if err != nil {
					klog.V(5).Infof("Error in deleting sbuscription target deploayble: %#v, err: %#v ", hubdpl, err)
					return err
				}
			}
		}
	}

	err = r.clearSubscriptionTargetDpl(sub)

	return err
}

// clearSubscriptionTargetDpls clear the subscription target deployable if exists.
func (r *ReconcileSubscription) clearSubscriptionTargetDpl(sub *appv1alpha1.Subscription) error {
	klog.V(5).Info("deleting sbscription target deploayble")

	// delete target deployable if exists.
	hubTargetDpl := &dplv1alpha1.Deployable{}
	err := r.Get(context.TODO(), types.NamespacedName{Name: sub.Name + "-target-deployable", Namespace: sub.Namespace}, hubTargetDpl)

	if err == nil {
		// check owner-reference and delete if it is generated.
		owners := hubTargetDpl.GetOwnerReferences()
		for _, owner := range owners {
			if owner.UID == sub.UID {
				err = r.Delete(context.TODO(), hubTargetDpl)
				if err != nil {
					klog.Infof("Error in deleting sbuscription target deploayble: %#v, err: %v", hubTargetDpl, err)
					return err
				}
			}
		}
	}

	return nil
}

func (r *ReconcileSubscription) updateSubAnnotations(sub *appv1alpha1.Subscription) map[string]string {
	subepanno := make(map[string]string)

	origsubanno := sub.GetAnnotations()

	// User and Group annotations
	subepanno[appv1alpha1.AnnotationUserIdentity] = strings.Trim(origsubanno[appv1alpha1.AnnotationUserIdentity], "")
	subepanno[appv1alpha1.AnnotationUserGroup] = strings.Trim(origsubanno[appv1alpha1.AnnotationUserGroup], "")

	// Keep Git related annotations from the source subscription.
	if !strings.EqualFold(origsubanno[appv1alpha1.AnnotationWebhookEventCount], "") {
		subepanno[appv1alpha1.AnnotationWebhookEventCount] = origsubanno[appv1alpha1.AnnotationWebhookEventCount]
	}

	if !strings.EqualFold(origsubanno[appv1alpha1.AnnotationGitBranch], "") {
		subepanno[appv1alpha1.AnnotationGitBranch] = origsubanno[appv1alpha1.AnnotationGitBranch]
	} else if !strings.EqualFold(origsubanno[appv1alpha1.AnnotationGithubBranch], "") {
		subepanno[appv1alpha1.AnnotationGitBranch] = origsubanno[appv1alpha1.AnnotationGithubBranch]
	}

	if !strings.EqualFold(origsubanno[appv1alpha1.AnnotationGitPath], "") {
		subepanno[appv1alpha1.AnnotationGitPath] = origsubanno[appv1alpha1.AnnotationGitPath]
	} else if !strings.EqualFold(origsubanno[appv1alpha1.AnnotationGithubPath], "") {
		subepanno[appv1alpha1.AnnotationGitPath] = origsubanno[appv1alpha1.AnnotationGithubPath]
	}

	if !strings.EqualFold(origsubanno[appv1alpha1.AnnotationBucketPath], "") {
		subepanno[appv1alpha1.AnnotationBucketPath] = origsubanno[appv1alpha1.AnnotationBucketPath]
	}

	if !strings.EqualFold(origsubanno[appv1alpha1.AnnotationClusterAdmin], "") {
		subepanno[appv1alpha1.AnnotationClusterAdmin] = origsubanno[appv1alpha1.AnnotationClusterAdmin]
	}

	if !strings.EqualFold(origsubanno[appv1alpha1.AnnotationCurrentNamespaceScoped], "") {
		subepanno[appv1alpha1.AnnotationCurrentNamespaceScoped] = origsubanno[appv1alpha1.AnnotationCurrentNamespaceScoped]
	}

	if !strings.EqualFold(origsubanno[appv1alpha1.AnnotationResourceReconcileOption], "") {
		subepanno[appv1alpha1.AnnotationResourceReconcileOption] = origsubanno[appv1alpha1.AnnotationResourceReconcileOption]
	}

	if !strings.EqualFold(origsubanno[appv1alpha1.AnnotationGitTargetCommit], "") {
		subepanno[appv1alpha1.AnnotationGitTargetCommit] = origsubanno[appv1alpha1.AnnotationGitTargetCommit]
	}

	if !strings.EqualFold(origsubanno[appv1alpha1.AnnotationGitTag], "") {
		subepanno[appv1alpha1.AnnotationGitTag] = origsubanno[appv1alpha1.AnnotationGitTag]
	}

	if !strings.EqualFold(origsubanno[appv1alpha1.AnnotationGitCloneDepth], "") {
		subepanno[appv1alpha1.AnnotationGitCloneDepth] = origsubanno[appv1alpha1.AnnotationGitCloneDepth]
	}

	if !strings.EqualFold(origsubanno[appv1alpha1.AnnotationResourceReconcileLevel], "") {
		subepanno[appv1alpha1.AnnotationResourceReconcileLevel] = origsubanno[appv1alpha1.AnnotationResourceReconcileLevel]
	}

	if !strings.EqualFold(origsubanno[appv1alpha1.AnnotationManualReconcileTime], "") {
		subepanno[appv1alpha1.AnnotationManualReconcileTime] = origsubanno[appv1alpha1.AnnotationManualReconcileTime]
	}

	// This is to verify cluster-admin annotation
	r.AddClusterAdminAnnotation(sub)

	// Add annotation for git path and branch
	// It is recommended to define Git path and branch in subscription annotations but
	// this code is to support those that already use ConfigMap.
	if sub.Spec.PackageFilter != nil && sub.Spec.PackageFilter.FilterRef != nil {
		subscriptionConfigMap := &corev1.ConfigMap{}
		subcfgkey := types.NamespacedName{
			Name:      sub.Spec.PackageFilter.FilterRef.Name,
			Namespace: sub.Namespace,
		}

		err := r.Get(context.TODO(), subcfgkey, subscriptionConfigMap)
		if err != nil {
			klog.Error("Failed to get PackageFilter.FilterRef of subsciption, error: ", err)
		} else {
			gitPath := subscriptionConfigMap.Data["path"]
			if gitPath != "" {
				subepanno[appv1alpha1.AnnotationGitPath] = gitPath
			}
			gitBranch := subscriptionConfigMap.Data["branch"]
			if gitBranch != "" {
				subepanno[appv1alpha1.AnnotationGitBranch] = gitBranch
			}
		}
	}

	return subepanno
}

func (r *ReconcileSubscription) updateSubscriptionStatus(sub *appv1alpha1.Subscription, clusterList []ManageClusters) {
	r.logger.Info(fmt.Sprintf("entry doMCMHubReconcile:updateSubscriptionStatus %s", PrintHelper(sub)))
	defer r.logger.Info(fmt.Sprintf("exit doMCMHubReconcile:updateSubscriptionStatus %s", PrintHelper(sub)))
	manifestName := "sub-" + sub.Name + "-manifest"
	newsubstatus := appv1alpha1.SubscriptionStatus{}

	newsubstatus.AnsibleJobsStatus = *sub.Status.AnsibleJobsStatus.DeepCopy()
	newsubstatus.Statuses = make(map[string]*appv1alpha1.SubscriptionPerClusterStatus)

	//msg := ""

	var curManifest manifestWorkV1.ManifestWork
	for _, cluster := range clusterList {
		err := r.Client.Get(context.TODO(), client.ObjectKey{Namespace: cluster.Cluster, Name: manifestName}, &curManifest)
		if err != nil {
			klog.Infof("Failed finding manifest in namespace %s with name %s for subscription status.",
				cluster.Cluster, manifestName)
			klog.Errorf("err is %e", err)
			return
		}

		mcsubstatus := &appv1alpha1.SubscriptionUnitStatus{}
		packages := &appv1alpha1.SubscriptionPerClusterStatus{}
		packages.SubscriptionPackageStatus = map[string]*appv1alpha1.SubscriptionUnitStatus{}

		for _, resourceStatus := range curManifest.Status.ResourceStatus.Manifests {
			mcsubstatus.LastUpdateTime = resourceStatus.Conditions[0].LastTransitionTime
			mcsubstatus.Message = resourceStatus.Conditions[0].Message
			mcsubstatus.Reason = resourceStatus.Conditions[0].Reason

			bytes, err := json.Marshal(resourceStatus.StatusFeedbacks)
			if err != nil {
				klog.Errorf("couldnt unmarshal resource when creating status - %e", err)
				return
			}

			mcsubstatus.ResourceStatus = &runtime.RawExtension{Raw: bytes}
			if resourceStatus.Conditions[1].Type == "Available" {
				mcsubstatus.Phase = appv1alpha1.SubscriptionSubscribed
			} else {
				mcsubstatus.Phase = appv1alpha1.SubscriptionFailed
			}

			packages.SubscriptionPackageStatus[resourceStatus.ResourceMeta.Name] = mcsubstatus
		}
		newsubstatus.Statuses[cluster.Cluster] = packages

		//if curManifest.Status.ResourceStatus != nil {
		//	mcsubstatus := &appv1alpha1.SubscriptionStatus{}
		//	err := json.Unmarshal(cstatus.ResourceStatus.Raw, mcsubstatus)
		//	if err != nil {
		//		klog.Infof("Failed to unmashall ResourceStatus from target cluster: %v, in deployable: %v/%v", cluster, found.GetNamespace(), found.GetName())
		//		return err
		//	}
		//
		//	if msg == "" {
		//		msg = fmt.Sprintf("%s:%s", cluster, mcsubstatus.Message)
		//	} else {
		//		msg += fmt.Sprintf(",%s:%s", cluster, mcsubstatus.Message)
		//	}
		//
		//	//get status per package if exist, for namespace/objectStore/helmRepo channel subscription status
		//	for _, lcStatus := range mcsubstatus.Statuses {
		//		for pkg, pkgStatus := range lcStatus.SubscriptionPackageStatus {
		//			subPkgStatus[pkg] = getStatusPerPackage(pkgStatus, chn)
		//		}
		//	}
		//
		//	//if no status per package, apply status.<per cluster>.resourceStatus, for github channel subscription status
		//	if len(subPkgStatus) == 0 {
		//		subUnitStatus := &appv1alpha1.SubscriptionUnitStatus{}
		//		subUnitStatus.LastUpdateTime = mcsubstatus.LastUpdateTime
		//		subUnitStatus.Phase = mcsubstatus.Phase
		//		subUnitStatus.Message = mcsubstatus.Message
		//		subUnitStatus.Reason = mcsubstatus.Reason
		//
		//		subPkgStatus["/"] = subUnitStatus
		//	}
		//}
		//
		//clusterSubStatus.SubscriptionPackageStatus = subPkgStatus
		//
		//newsubstatus.Statuses[cluster] = clusterSubStatus
	}

	newsubstatus.Phase = appv1alpha1.SubscriptionPropagated
	newsubstatus.LastUpdateTime = sub.Status.LastUpdateTime
	klog.V(5).Info("Check status for ", sub.Namespace, "/", sub.Name, " with ", newsubstatus)
	newsubstatus.Message = "cluster5:active"

	if !utils.IsEqualSubScriptionStatus(&sub.Status, &newsubstatus) {
		klog.V(1).Infof("check subscription status sub: %v/%v, substatus: %#v, newsubstatus: %#v",
			sub.Namespace, sub.Name, sub.Status, newsubstatus)

		//perserve the Ansiblejob status
		newsubstatus.DeepCopyInto(&sub.Status)
	}
}

func getStatusPerPackage(pkgStatus *appv1alpha1.SubscriptionUnitStatus, chn *chnv1alpha1.Channel) *appv1alpha1.SubscriptionUnitStatus {
	subUnitStatus := &appv1alpha1.SubscriptionUnitStatus{}

	switch chn.Spec.Type {
	case "HelmRepo":
		subUnitStatus.LastUpdateTime = pkgStatus.LastUpdateTime

		if pkgStatus.ResourceStatus != nil {
			setHelmSubUnitStatus(pkgStatus.ResourceStatus, subUnitStatus)
		} else {
			subUnitStatus.Phase = pkgStatus.Phase
			subUnitStatus.Message = pkgStatus.Message
			subUnitStatus.Reason = pkgStatus.Reason
		}
	default:
		subUnitStatus = pkgStatus
	}

	return subUnitStatus
}

func setHelmSubUnitStatus(pkgResourceStatus *runtime.RawExtension, subUnitStatus *appv1alpha1.SubscriptionUnitStatus) {
	if pkgResourceStatus == nil || subUnitStatus == nil {
		klog.Errorf("failed to setHelmSubUnitStatus due to pkgResourceStatus %v or subUnitStatus %v is nil", pkgResourceStatus, subUnitStatus)
		return
	}

	helmAppStatus := &releasev1.HelmAppStatus{}
	err := json.Unmarshal(pkgResourceStatus.Raw, helmAppStatus)

	if err != nil {
		klog.Error("Failed to unmashall pkgResourceStatus to helm condition. err: ", err)
	}

	subUnitStatus.Phase = "Subscribed"
	subUnitStatus.Message = ""
	subUnitStatus.Reason = ""

	messages := []string{}
	reasons := []string{}

	for _, condition := range helmAppStatus.Conditions {
		if strings.Contains(string(condition.Reason), "Error") {
			subUnitStatus.Phase = "Failed"

			messages = append(messages, condition.Message)

			reasons = append(reasons, string(condition.Reason))
		}
	}

	if len(messages) > 0 {
		subUnitStatus.Message = strings.Join(messages, ", ")
	}

	if len(reasons) > 0 {
		subUnitStatus.Reason = strings.Join(reasons, ", ")
	}
}

func (r *ReconcileSubscription) getSubscriptionDeployables(sub *appv1alpha1.Subscription) map[string]*dplv1alpha1.Deployable {
	allDpls := make(map[string]*dplv1alpha1.Deployable)

	dplList := &dplv1alpha1.DeployableList{}

	chNameSpace, chName, chType := r.GetChannelNamespaceType(sub)

	dplNamespace := chNameSpace

	if utils.IsGitChannel(chType) {
		// If Git channel, deployables are created in the subscription namespace
		dplNamespace = sub.Namespace
	}

	dplListOptions := &client.ListOptions{Namespace: dplNamespace}

	if sub.Spec.PackageFilter != nil && sub.Spec.PackageFilter.LabelSelector != nil {
		matchLbls := sub.Spec.PackageFilter.LabelSelector.MatchLabels
		matchLbls[chnv1alpha1.KeyChannel] = chName
		matchLbls[chnv1alpha1.KeyChannelType] = chType
		clSelector, err := dplutils.ConvertLabels(sub.Spec.PackageFilter.LabelSelector)

		if err != nil {
			klog.Error("Failed to set label selector of subscrption:", sub.Spec.PackageFilter.LabelSelector, " err: ", err)
			return nil
		}

		dplListOptions.LabelSelector = clSelector
	} else {
		// Handle deployables from multiple channels in the same namespace
		subLabel := make(map[string]string)
		subLabel[chnv1alpha1.KeyChannel] = chName
		subLabel[chnv1alpha1.KeyChannelType] = chType

		if utils.IsGitChannel(chType) {
			subscriptionNameLabel := types.NamespacedName{
				Name:      sub.Name,
				Namespace: sub.Namespace,
			}
			subscriptionNameLabelStr := strings.ReplaceAll(subscriptionNameLabel.String(), "/", "-")

			// subscription name can be longer than 63 characters because it is applicationName + -subscription-n. A label cannot exceed 63 chars.
			subLabel[appv1alpha1.LabelSubscriptionName] = utils.ValidateK8sLabel(utils.TrimLabelLast63Chars(subscriptionNameLabelStr))
		}

		labelSelector := &metav1.LabelSelector{
			MatchLabels: subLabel,
		}
		chSelector, err := dplutils.ConvertLabels(labelSelector)

		if err != nil {
			klog.Error("Failed to set label selector. err: ", chSelector, " err: ", err)
			return nil
		}

		dplListOptions.LabelSelector = chSelector
	}

	// Sleep so that all deployables are fully created
	time.Sleep(3 * time.Second)

	err := r.Client.List(context.TODO(), dplList, dplListOptions)

	if err != nil {
		klog.Error("Failed to list objects from sbuscription namespace ", sub.Namespace, " err: ", err)
		return nil
	}

	klog.V(1).Info("Hub Subscription found Deployables:", len(dplList.Items))

	for _, dpl := range dplList.Items {
		if !r.checkDeployableBySubcriptionPackageFilter(sub, dpl) {
			continue
		}

		dplkey := types.NamespacedName{Name: dpl.Name, Namespace: dpl.Namespace}.String()
		allDpls[dplkey] = dpl.DeepCopy()
	}

	return allDpls
}

func checkDplPackageName(sub *appv1alpha1.Subscription, dpl dplv1alpha1.Deployable) bool {
	dpltemplate := &unstructured.Unstructured{}

	if dpl.Spec.Template != nil {
		err := json.Unmarshal(dpl.Spec.Template.Raw, dpltemplate)
		if err == nil {
			dplPkgName := dpltemplate.GetName()
			if sub.Spec.Package != "" && sub.Spec.Package != dplPkgName {
				klog.Info("Name does not match, skiping:", sub.Spec.Package, "|", dplPkgName)
				return false
			}
		}
	}

	return true
}

func (r *ReconcileSubscription) checkDeployableBySubcriptionPackageFilter(sub *appv1alpha1.Subscription, dpl dplv1alpha1.Deployable) bool {
	dplanno := dpl.GetAnnotations()

	if sub.Spec.PackageFilter != nil {
		if dplanno == nil {
			dplanno = make(map[string]string)
		}

		if !checkDplPackageName(sub, dpl) {
			return false
		}

		annotations := sub.Spec.PackageFilter.Annotations

		//append deployable template annotations to deployable annotations only if they don't exist in the deployable annotations
		dpltemplate := &unstructured.Unstructured{}

		if dpl.Spec.Template != nil {
			err := json.Unmarshal(dpl.Spec.Template.Raw, dpltemplate)
			if err == nil {
				dplTemplateAnno := dpltemplate.GetAnnotations()
				for k, v := range dplTemplateAnno {
					if dplanno[k] == "" {
						dplanno[k] = v
					}
				}
			}
		}

		vdpl := dpl.GetAnnotations()[dplv1alpha1.AnnotationDeployableVersion]

		klog.V(5).Info("checking annotations package filter: ", annotations)

		if annotations != nil {
			matched := true

			for k, v := range annotations {
				if dplanno[k] != v {
					matched = false
					break
				}
			}

			if !matched {
				return false
			}
		}

		vsub := sub.Spec.PackageFilter.Version
		if vsub != "" {
			vmatch := subutil.SemverCheck(vsub, vdpl)
			klog.V(5).Infof("version check is %v; subscription version filter condition is %v, deployable version is: %v", vmatch, vsub, vdpl)

			if !vmatch {
				return false
			}
		}
	}

	return true
}

func (r *ReconcileSubscription) updateTargetSubscriptionDeployable(sub *appv1alpha1.Subscription, targetSubDpl *dplv1alpha1.Deployable) error {
	targetKey := types.NamespacedName{
		Namespace: targetSubDpl.Namespace,
		Name:      targetSubDpl.Name,
	}

	found := &dplv1alpha1.Deployable{}
	err := r.Get(context.TODO(), targetKey, found)

	if err != nil && apierrors.IsNotFound(err) {
		klog.Info("Creating target Deployable - ", "namespace: ", targetSubDpl.Namespace, ", name: ", targetSubDpl.Name)
		err = r.Create(context.TODO(), targetSubDpl)

		//record events
		addtionalMsg := "target Depolyable " + targetKey.String() + " created in the subscription namespace"
		r.eventRecorder.RecordEvent(sub, "Deploy", addtionalMsg, err)

		return err
	} else if err != nil {
		return err
	}

	orgTpl := &unstructured.Unstructured{}
	err = json.Unmarshal(targetSubDpl.Spec.Template.Raw, orgTpl)

	if err != nil {
		klog.V(5).Info("Error in unmarshall target subscription deployable template, err:", err, " |template: ", string(targetSubDpl.Spec.Template.Raw))
		return err
	}

	fndTpl := &unstructured.Unstructured{}
	err = json.Unmarshal(found.Spec.Template.Raw, fndTpl)

	if err != nil {
		klog.V(5).Info("Error in unmarshall target found subscription deployable template, err:", err, " |template: ", string(found.Spec.Template.Raw))
		return err
	}

	if !reflect.DeepEqual(orgTpl, fndTpl) || !reflect.DeepEqual(targetSubDpl.Spec.Overrides, found.Spec.Overrides) {
		klog.V(5).Infof("Updating target Deployable. orig: %#v, found: %#v", targetSubDpl, found)

		targetSubDpl.Spec.DeepCopyInto(&found.Spec)

		foundanno := found.GetAnnotations()
		if foundanno == nil {
			foundanno = make(map[string]string)
		}

		foundanno[dplv1alpha1.AnnotationIsGenerated] = "true"
		foundanno[dplv1alpha1.AnnotationLocal] = "false"
		found.SetAnnotations(foundanno)

		klog.V(5).Info("Updating Deployable - ", "namespace: ", targetSubDpl.Namespace, " ,name: ", targetSubDpl.Name)

		err = r.Update(context.TODO(), found)

		//record events
		addtionalMsg := "target Depolyable " + targetKey.String() + " updated in the subscription namespace"
		r.eventRecorder.RecordEvent(sub, "Deploy", addtionalMsg, err)

		if err != nil {
			return err
		}
	}

	return nil
}

func (r *ReconcileSubscription) initObjectStore(channel *chnv1alpha1.Channel) (*awsutils.Handler, string, error) {
	var err error

	awshandler := &awsutils.Handler{}

	pathName := channel.Spec.Pathname

	if pathName == "" {
		errmsg := "Empty Pathname in channel " + channel.Spec.Pathname
		klog.Error(errmsg)

		return nil, "", errors.New(errmsg)
	}

	if strings.HasSuffix(pathName, "/") {
		last := len(pathName) - 1
		pathName = pathName[:last]
	}

	loc := strings.LastIndex(pathName, "/")
	endpoint := pathName[:loc]
	bucket := pathName[loc+1:]

	accessKeyID := ""
	secretAccessKey := ""
	region := ""

	if channel.Spec.SecretRef != nil {
		channelSecret := &corev1.Secret{}
		chnseckey := types.NamespacedName{
			Name:      channel.Spec.SecretRef.Name,
			Namespace: channel.Namespace,
		}

		if err := r.Get(context.TODO(), chnseckey, channelSecret); err != nil {
			return nil, "", gerr.Wrap(err, "failed to get reference secret from channel")
		}

		err = yaml.Unmarshal(channelSecret.Data[awsutils.SecretMapKeyAccessKeyID], &accessKeyID)
		if err != nil {
			klog.Error("Failed to unmashall accessKey from secret with error:", err)

			return nil, "", err
		}

		err = yaml.Unmarshal(channelSecret.Data[awsutils.SecretMapKeySecretAccessKey], &secretAccessKey)
		if err != nil {
			klog.Error("Failed to unmashall secretaccessKey from secret with error:", err)

			return nil, "", err
		}

		regionData := channelSecret.Data[awsutils.SecretMapKeyRegion]

		if len(regionData) > 0 {
			err = yaml.Unmarshal(regionData, &region)
			if err != nil {
				klog.Error("Failed to unmashall region from secret with error:", err)

				return nil, "", err
			}
		}
	}

	klog.V(1).Info("Trying to connect to object bucket ", endpoint, "|", bucket)

	if err := awshandler.InitObjectStoreConnection(endpoint, accessKeyID, secretAccessKey, region); err != nil {
		klog.Error(err, "unable initialize object store settings")

		return nil, "", err
	}
	// Check whether the connection is setup successfully
	if err := awshandler.Exists(bucket); err != nil {
		klog.Error(err, "Unable to access object store bucket ", bucket, " for channel ", channel.Name)

		return nil, "", err
	}

	return awshandler, bucket, nil
}

func (r *ReconcileSubscription) updateObjectBucketAnnotation(
	sub *appv1alpha1.Subscription, channel, secondaryChannel *chnv1alpha1.Channel, parentType string) (bool, error) {
	awsHandler, bucket, err := r.initObjectStore(channel)
	if err != nil {
		klog.Error(err, "Unable to access object store: ")

		if secondaryChannel != nil {
			klog.Infof("trying the secondary channel %s", secondaryChannel.Name)
			// Try with secondary channel
			awsHandler, bucket, err = r.initObjectStore(secondaryChannel)

			if err != nil {
				klog.Error(err, "Unable to access object store with channel ", channel.Name)

				return false, err
			}
		} else {
			klog.Error(err, "Unable to access object store with channel ", channel.Name)

			return false, err
		}
	}

	var folderName *string

	annotations := sub.GetAnnotations()
	bucketPath := annotations[appv1.AnnotationBucketPath]

	if bucketPath != "" {
		folderName = &bucketPath
	}

	keys, err := awsHandler.List(bucket, folderName)
	klog.V(5).Infof("object keys: %v", keys)

	if err != nil {
		klog.Error("Failed to list objects in bucket ", bucket)

		return false, err
	}

	allDpls := make(map[string]*dplv1alpha1.Deployable)

	// converting template from obeject store to DPL
	for _, key := range keys {
		tplb, err := awsHandler.Get(bucket, key)
		if err != nil {
			klog.Error("Failed to get object ", key, " in bucket ", bucket)

			return false, err
		}

		// skip empty body object store
		if len(tplb.Content) == 0 {
			continue
		}

		dpl := &dplv1.Deployable{}
		dpl.Name = generateDplNameFromKey(key)
		dpl.Namespace = bucket
		dpl.Spec.Template = &runtime.RawExtension{}
		dpl.GenerateName = tplb.GenerateName
		verionAnno := map[string]string{dplv1.AnnotationDeployableVersion: tplb.Version}
		dpl.SetAnnotations(verionAnno)
		err = yaml.Unmarshal(tplb.Content, dpl.Spec.Template)

		if err != nil {
			klog.Error("Failed to unmashall ", bucket, "/", key, " err:", err)
			continue
		}

		klog.V(5).Infof("Retived Dpl: %v", dpl)

		dplkey := types.NamespacedName{Name: dpl.Name, Namespace: dpl.Namespace}.String()
		allDpls[dplkey] = dpl
	}

	// Check and add cluster-admin annotation for multi-namepsace application
	updated := r.AddClusterAdminAnnotation(sub)

	topoFlag := extracResourceListFromDeployables(sub, allDpls, parentType)

	return (updated || topoFlag), nil
}

func generateDplNameFromKey(key string) string {
	return strings.ReplaceAll(key, "/", "-")
}

// difference returns the elements in `a` that aren't in `b`.
func difference(a []string, b []ManageClusters) []string {
	mb := make(map[string]struct{}, len(b))
	for _, x := range b {
		mb[x.Cluster] = struct{}{}
	}
	var diff []string
	for _, x := range a {
		if _, found := mb[x]; !found {
			diff = append(diff, x)
		}
	}
	return diff
}
