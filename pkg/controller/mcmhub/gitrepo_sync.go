// Copyright 2020 The Kubernetes Authors.
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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"

	"github.com/ghodss/yaml"
	"github.com/pkg/errors"
	gerr "github.com/pkg/errors"
	"helm.sh/helm/v3/pkg/repo"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog"

	chnv1 "github.com/open-cluster-management/multicloud-operators-channel/pkg/apis/apps/v1"
	appv1 "github.com/open-cluster-management/multicloud-operators-subscription/pkg/apis/apps/v1"
	helmops "github.com/open-cluster-management/multicloud-operators-subscription/pkg/subscriber/helmrepo"

	"github.com/open-cluster-management/multicloud-operators-subscription/pkg/utils"
)

type kubeResource struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   *kubeResourceMetadata
}

type kubeResourceMetadata struct {
	Name      string `yaml:"name"`
	Namespace string `yaml:"namespace"`
}

// GetGitResources clones the git repo and regenerate deployables and update annotation if needed
func (r *ReconcileSubscription) GetGitResources(sub *appv1.Subscription) ([]*v1.ObjectReference, error) {
	var objRefList []*v1.ObjectReference
	origsub := &appv1.Subscription{}
	sub.DeepCopyInto(origsub)

	primaryChannel, _, err := r.getChannel(sub)

	if err != nil {
		klog.Errorf("Failed to find a channel for subscription: %s", sub.GetName())
		return nil, err
	}

	if utils.IsGitChannel(string(primaryChannel.Spec.Type)) {
		klog.Infof("Subscription %s has Git type channel.", sub.GetName())

		//making sure the commit id is coming from the same source
		commit, err := r.hubGitOps.GetLatestCommitID(sub)
		if err != nil {
			klog.Error(err.Error())
			return nil, err
		}

		annotations := sub.GetAnnotations()
		if annotations == nil {
			annotations = make(map[string]string)
			sub.SetAnnotations(annotations)
		}

		subKey := types.NamespacedName{Name: sub.GetName(), Namespace: sub.GetNamespace()}
		// Compare the commit to the Git repo and update manifests only if the commit has changed
		// If subscription does not have commit annotation, it needs to be generated in this block.
		oldCommit := getCommitID(sub)

		if oldCommit == "" || !strings.EqualFold(oldCommit, commit) ||
			r.isHookUpdate(annotations, subKey) {
			if oldCommit == "" || !strings.EqualFold(oldCommit, commit) {
				klog.Infof("The Git commit has changed since the last reconcile. last: %s, new: %s", oldCommit, commit)
			}

			if r.isHookUpdate(annotations, subKey) {
				klog.Info("The topo annotation does not have applied hooks. Adding it.")
			}

			baseDir := r.hubGitOps.GetRepoRootDirctory(sub)
			resourcePath := getResourcePath(r.hubGitOps.ResolveLocalGitFolder, sub)

			// Check and add cluster-admin annotation for multi-namepsace application
			isAdmin := r.AddClusterAdminAnnotation(sub)

			objRefList, err = r.processRepo(primaryChannel, sub, r.hubGitOps.ResolveLocalGitFolder(sub), resourcePath, baseDir, isAdmin)
			if err != nil {
				klog.Error(err.Error())
				return nil, err
			}

			setCommitID(sub, commit)
		} else {
			klog.Infof("The Git commit has not changed since the last reconcile. last: %s, new: %s", annotations[appv1.AnnotationGitCommit], commit)
			return nil, nil
		}
	}

	return objRefList, nil
}

func (r *ReconcileSubscription) isHookUpdate(a map[string]string, subKey types.NamespacedName) bool {
	applied := r.hooks.GetLastAppliedInstance(subKey)

	if len(applied.pre) != 0 && !strings.Contains(a[appv1.AnnotationTopo], applied.pre) {
		return true
	}

	if len(applied.post) != 0 && !strings.Contains(a[appv1.AnnotationTopo], applied.post) {
		return true
	}

	return false
}

// AddClusterAdminAnnotation adds cluster-admin annotation if conditions are met
func (r *ReconcileSubscription) AddClusterAdminAnnotation(sub *appv1.Subscription) bool {
	annotations := sub.GetAnnotations()
	if annotations[appv1.AnnotationHosting] == "" {
		// if there is hosting subscription, the cluster admin annotation must have been inherited. Don't remove.
		delete(annotations, appv1.AnnotationClusterAdmin) // make sure cluster-admin annotation is removed to begin with
	}

	if utils.IsClusterAdmin(r.Client, sub, r.eventRecorder) {
		annotations[appv1.AnnotationClusterAdmin] = "true"
		sub.SetAnnotations(annotations)

		return true
	}

	return false
}

func getResourcePath(localFolderFunc func(*appv1.Subscription) string, sub *appv1.Subscription) string {
	resourcePath := localFolderFunc(sub)

	annotations := sub.GetAnnotations()
	if annotations[appv1.AnnotationGithubPath] != "" {
		resourcePath = filepath.Join(localFolderFunc(sub), annotations[appv1.AnnotationGithubPath])
	} else if annotations[appv1.AnnotationGitPath] != "" {
		resourcePath = filepath.Join(localFolderFunc(sub), annotations[appv1.AnnotationGitPath])
	}

	return resourcePath
}

func getGitChart(sub *appv1.Subscription, localRepoRoot, subPath string) (*repo.IndexFile, error) {
	chartDirs, a, b, c, d, err := utils.SortResources(localRepoRoot, subPath)
	if err != nil {
		return nil, gerr.Wrap(err, "failed to get helm index for topo annotation")
	}

	//to pass the linter without changing the utils.SortResources() API
	_ = fmt.Sprint(a, b, c, d)

	// Build a helm repo index file
	indexFile, err := utils.GenerateHelmIndexFile(sub, localRepoRoot, chartDirs)

	if err != nil {
		// If package name is not specified in the subscription, filterCharts throws an error. In this case, just return the original index file.
		return nil, gerr.Wrap(err, "failed to get helm index file")
	}

	return indexFile, nil
}

func (r *ReconcileSubscription) gitHelmResourceString(sub *appv1.Subscription, chn, secondChn *chnv1.Channel) string {
	idxFile, err := getGitChart(sub, utils.GetLocalGitFolder(sub), getResourcePath(r.hubGitOps.ResolveLocalGitFolder, sub))
	if err != nil {
		klog.Error(err.Error())
		return ""
	}

	_ = idxFile

	if len(idxFile.Entries) != 0 {
		rls, err := helmops.ChartIndexToHelmReleases(r.Client, chn, secondChn, sub, idxFile)
		if err != nil {
			klog.Error(err.Error())
			return ""
		}

		res, err := generateResrouceList(r.cfg, rls)
		if err != nil {
			klog.Error(err.Error())
			return ""
		}

		return res
	}

	return ""
}

func (r *ReconcileSubscription) processRepo(chn *chnv1.Channel, sub *appv1.Subscription,
	localRepoRoot, subPath, baseDir string, isAdmin bool) ([]*v1.ObjectReference, error) {
	chartDirs, kustomizeDirs, crdsAndNamespaceFiles, rbacFiles, otherFiles, err := utils.SortResources(localRepoRoot, subPath)

	if err != nil {
		klog.Error(err, "Failed to sort kubernetes resources and helm charts.")

		return nil, err
	}

	// Build a helm repo index file
	indexFile, err := utils.GenerateHelmIndexFile(sub, localRepoRoot, chartDirs)

	if err != nil {
		// If package name is not specified in the subscription, filterCharts throws an error. In this case, just return the original index file.
		klog.Error(err, "Failed to generate helm index file.")

		return nil, err
	}

	b, _ := yaml.Marshal(indexFile)
	klog.Info("New index file ", string(b))

	// Get object reference map for all the kube resources and helm charts from the git repo
	errMessage := ""
	objRefMap := make(map[v1.ObjectReference]*v1.ObjectReference)

	err = r.subscribeResources(chn, sub, crdsAndNamespaceFiles, baseDir, objRefMap)
	if err != nil {
		errMessage += err.Error() + "/n"
	}

	err = r.subscribeResources(chn, sub, rbacFiles, baseDir, objRefMap)
	if err != nil {
		errMessage += err.Error() + "/n"
	}

	err = r.subscribeResources(chn, sub, otherFiles, baseDir, objRefMap)
	if err != nil {
		errMessage += err.Error() + "/n"
	}

	err = r.subscribeKustomizations(chn, sub, kustomizeDirs, baseDir, objRefMap)
	if err != nil {
		errMessage += err.Error() + "/n"
	}

	err = r.subscribeHelmCharts(chn, sub, indexFile, objRefMap)
	if err != nil {
		errMessage += err.Error() + "/n"
	}

	if errMessage != "" {
		return nil, errors.New(errMessage)
	}

	// Get list of object references from the map
	objRefList := []*v1.ObjectReference{}

	for _, value := range objRefMap {
		// No need to save the namespace object to the resource list of the appsub
		if value.Kind == "Namespace" {
			continue
		}

		// respect object customized namespace if the appsub user is subscription admin, or apply it to appsub namespace
		if isAdmin {
			if value.Namespace == "" {
				value.Namespace = sub.Namespace
			}
		} else {
			value.Namespace = sub.Namespace
		}

		objRefList = append(objRefList, value)
	}

	return objRefList, nil
}

func (r *ReconcileSubscription) subscribeResources(chn *chnv1.Channel, sub *appv1.Subscription,
	rscFiles []string, baseDir string, objRefMap map[v1.ObjectReference]*v1.ObjectReference) error {
	// sync kube resource manifests
	for _, rscFile := range rscFiles {
		file, err := ioutil.ReadFile(rscFile) // #nosec G304 rscFile is not user input

		if err != nil {
			klog.Error(err, "Failed to read YAML file "+rscFile)
			continue
		}

		//skip pre/posthook folder
		dir, _ := filepath.Split(rscFile)

		if strings.HasSuffix(dir, PrehookDirSuffix) || strings.HasSuffix(dir, PosthookDirSuffix) {
			continue
		}

		klog.Info("Processing ... " + rscFile)

		resourceDir := strings.TrimPrefix(dir, baseDir)
		resourceDir = strings.Trim(resourceDir, "/")

		resources := utils.ParseKubeResoures(file)

		if len(resources) > 0 {
			for _, resource := range resources {
				if err := r.addObjectReference(objRefMap, resource); err != nil {
					klog.Error("Failed to generate object reference", err)
					return err
				}
			}
		}
	}

	return nil
}

func (r *ReconcileSubscription) subscribeKustomizations(chn *chnv1.Channel, sub *appv1.Subscription, kustomizeDirs map[string]string,
	baseDir string, objRefMap map[v1.ObjectReference]*v1.ObjectReference) error {
	for _, kustomizeDir := range kustomizeDirs {
		klog.Info("Applying kustomization ", kustomizeDir)

		relativePath := kustomizeDir

		if len(strings.SplitAfter(kustomizeDir, baseDir+"/")) > 1 {
			relativePath = strings.SplitAfter(kustomizeDir, baseDir+"/")[1]
		}

		utils.VerifyAndOverrideKustomize(sub.Spec.PackageOverrides, relativePath, kustomizeDir)

		out, err := utils.RunKustomizeBuild(kustomizeDir)

		if err != nil {
			klog.Error("Failed to applying kustomization, error: ", err.Error())
			return err
		}

		// Split the output of kustomize build output into individual kube resource YAML files
		resources := utils.ParseYAML(out)
		for _, resource := range resources {
			resourceFile := []byte(strings.Trim(resource, "\t \n"))

			t := kubeResource{}
			err := yaml.Unmarshal(resourceFile, &t)

			if err != nil {
				klog.Error(err, "Failed to unmarshal YAML file")
				continue
			}

			if t.APIVersion == "" || t.Kind == "" {
				klog.Info("Not a Kubernetes resource")
			} else {
				if err := r.addObjectReference(objRefMap, resourceFile); err != nil {
					klog.Error("Failed to generate object reference", err)
					return err
				}
			}
		}
	}

	return nil
}

type helmSpec struct {
	ChartName   string      `json:"chartName,omitempty"`
	ReleaseName string      `json:"releaseName,omitempty"`
	Version     string      `json:"version,omitempty"`
	Source      *helmSource `json:"source,omitempty"`
}

type helmSource struct {
	HelmRepo *sourceURLs `json:"helmRepo,omitempty"`
	Git      *sourceURLs `json:"git,omitempty"`
	Type     string      `json:"type,omitempty"`
}

type sourceURLs struct {
	URLs      []string `json:"urls,omitempty"`
	ChartPath string   `json:"chartPath,omitempty"`
}

func (r *ReconcileSubscription) subscribeHelmCharts(chn *chnv1.Channel, sub *appv1.Subscription, indexFile *repo.IndexFile,
	objRefMap map[v1.ObjectReference]*v1.ObjectReference) error {
	for packageName, chartVersions := range indexFile.Entries {
		klog.Infof("chart: %s\n%v", packageName, chartVersions)

		obj := &unstructured.Unstructured{}
		obj.SetKind("HelmRelease")
		obj.SetAPIVersion("apps.open-cluster-management.io/v1")
		obj.SetName(packageName + "-" + chartVersions[0].Version)

		spec := &helmSpec{}
		spec.ChartName = packageName
		spec.ReleaseName = packageName
		spec.Version = chartVersions[0].Version

		sourceurls := &sourceURLs{}
		sourceurls.URLs = []string{chn.Spec.Pathname}

		src := &helmSource{}

		src.Type = chnv1.ChannelTypeGit
		src.Git = sourceurls
		chartVersion, _ := indexFile.Get(packageName, chartVersions[0].Version)
		src.Git.ChartPath = chartVersion.URLs[0]

		spec.Source = src

		obj.Object["spec"] = spec

		dplSpec, err := json.Marshal(obj)
		if err != nil {
			klog.Error("failed to marshal helmrelease spec")
			return err
		}

		klog.V(2).Info("Generating object reference")

		if err := r.addObjectReference(objRefMap, dplSpec); err != nil {
			klog.Error("Failed to generate object reference", err)
			return err
		}

	}

	return nil
}

func (r *ReconcileSubscription) addObjectReference(objRefMap map[v1.ObjectReference]*v1.ObjectReference, filecontent []byte) error {
	obj := &unstructured.Unstructured{}
	if err := yaml.Unmarshal(filecontent, obj); err != nil {
		klog.Error("Failed to unmarshal resource YAML.")
		return err
	}

	objRef := &v1.ObjectReference{
		Kind:       obj.GetKind(),
		Namespace:  obj.GetNamespace(),
		Name:       obj.GetName(),
		APIVersion: obj.GetAPIVersion(),
	}

	objRefMap[*objRef] = objRef

	return nil
}
