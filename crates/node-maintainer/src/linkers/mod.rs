#[cfg(not(target_arch = "wasm32"))]
mod hoisted;
#[cfg(not(target_arch = "wasm32"))]
mod isolated;

#[cfg(not(target_arch = "wasm32"))]
use std::path::{Path, PathBuf};

#[cfg(not(target_arch = "wasm32"))]
use hoisted::HoistedLinker;
#[cfg(not(target_arch = "wasm32"))]
use isolated::IsolatedLinker;

#[cfg(not(target_arch = "wasm32"))]
use crate::{
    graph::Graph, Lockfile, NodeMaintainerError, ProgressHandler, PruneProgress, ScriptLineHandler,
    ScriptStartHandler,
};

#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct LinkerOptions {
    pub(crate) concurrency: usize,
    pub(crate) actual_tree: Option<Lockfile>,
    pub(crate) script_concurrency: usize,
    pub(crate) cache: Option<PathBuf>,
    pub(crate) prefer_copy: bool,
    pub(crate) validate: bool,
    pub(crate) root: PathBuf,
    pub(crate) on_prune_progress: Option<PruneProgress>,
    pub(crate) on_extract_progress: Option<ProgressHandler>,
    pub(crate) on_script_start: Option<ScriptStartHandler>,
    pub(crate) on_script_line: Option<ScriptLineHandler>,
}

pub(crate) enum Linker {
    #[cfg(not(target_arch = "wasm32"))]
    Isolated(IsolatedLinker),
    #[cfg(not(target_arch = "wasm32"))]
    Hoisted(HoistedLinker),
    #[allow(dead_code)]
    Null,
}

impl Linker {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn isolated(opts: LinkerOptions) -> Self {
        Self::Isolated(IsolatedLinker::new(opts))
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn hoisted(opts: LinkerOptions) -> Self {
        Self::Hoisted(HoistedLinker::new(opts))
    }

    #[allow(dead_code)]
    pub fn null() -> Self {
        Self::Null
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn prune(
        &self,
        #[allow(dead_code)] graph: &Graph,
    ) -> Result<usize, NodeMaintainerError> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::Isolated(isolated) => isolated.prune(graph).await,
            #[cfg(not(target_arch = "wasm32"))]
            Self::Hoisted(hoisted) => hoisted.prune(graph).await,
            Self::Null => Ok(0),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn extract(
        &self,
        #[allow(dead_code)] graph: &Graph,
    ) -> Result<usize, NodeMaintainerError> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::Isolated(isolated) => isolated.extract(graph).await,
            #[cfg(not(target_arch = "wasm32"))]
            Self::Hoisted(hoisted) => hoisted.extract(graph).await,
            Self::Null => Ok(0),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn rebuild(
        &self,
        graph: &Graph,
        ignore_scripts: bool,
    ) -> Result<(), NodeMaintainerError> {
        tracing::debug!("Running lifecycle scripts...");
        let start = std::time::Instant::now();
        if !ignore_scripts {
            self.run_scripts(graph, "preinstall").await?;
        }
        self.link_bins(graph).await?;
        if !ignore_scripts {
            self.run_scripts(graph, "install").await?;
            self.run_scripts(graph, "postinstall").await?;
        }
        tracing::debug!(
            "Ran lifecycle scripts in {}ms.",
            start.elapsed().as_millis()
        );
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn link_bins(
        &self,
        #[allow(dead_code)] graph: &Graph,
    ) -> Result<usize, NodeMaintainerError> {
        tracing::debug!("Linking bins...");
        let start = std::time::Instant::now();
        let linked = match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::Isolated(isolated) => isolated.link_bins(graph).await,
            #[cfg(not(target_arch = "wasm32"))]
            Self::Hoisted(hoisted) => hoisted.link_bins(graph).await,
            Self::Null => Ok(0),
        }?;
        tracing::debug!(
            "Linked {linked} package bins in {}ms.",
            start.elapsed().as_millis()
        );
        Ok(linked)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn run_scripts(
        &self,
        #[allow(dead_code)] graph: &Graph,
        #[allow(dead_code)] event: &str,
    ) -> Result<(), NodeMaintainerError> {
        use std::io::{BufReader, BufRead};

        use futures::{StreamExt, TryStreamExt};
        use oro_common::BuildManifest;
        use oro_script::OroScript;

        let (pending_rebuild, opts) = match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::Isolated(isolated) => (&isolated.pending_rebuild, &isolated.opts),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Hoisted(hoisted) => (&hoisted.pending_rebuild, &hoisted.opts),
            Self::Null => return Ok(())
        };
        let start = std::time::Instant::now();
        tracing::debug!("Running {event} lifecycle scripts");
        let root = &opts.root;
        futures::stream::iter(pending_rebuild.lock().await.iter().copied())
            .map(Ok)
            .try_for_each_concurrent(opts.script_concurrency, move |idx| async move {
                let (pkg_dir, workspace_path) = if idx == graph.root {
                    (root.clone(), root.clone())
                } else {
                    match self {
                        #[cfg(not(target_arch = "wasm32"))]
                        Self::Isolated(isolated) => isolated.package_dir(graph, idx),
                        #[cfg(not(target_arch = "wasm32"))]
                        Self::Hoisted(hoisted) => hoisted.package_dir(graph, idx),
                        Self::Null => return Ok(())
                    }
                };

                let is_optional = graph.is_optional(idx);

                let build_mani =
                    BuildManifest::from_path(pkg_dir.join("package.json")).map_err(|e| {
                        NodeMaintainerError::BuildManifestReadError(pkg_dir.join("package.json"), e)
                    })?;

                let name = graph[idx].package.name().to_string();
                if build_mani.scripts.contains_key(event) {
                    let package_dir = pkg_dir.clone();
                    let event = event.to_owned();
                    let event_clone = event.clone();
                    let span = tracing::info_span!("script");
                    let _span_enter = span.enter();
                    if let Some(on_script_start) = &opts.on_script_start {
                        on_script_start(&graph[idx].package, &event);
                    }
                    std::mem::drop(_span_enter);
                    let mut script = match async_std::task::spawn_blocking(move || {
                        OroScript::new(package_dir, event_clone)?
                            .workspace_path(workspace_path)
                            .spawn()
                    })
                    .await
                    {
                        Ok(script) => script,
                        Err(e) if is_optional => {
                            let e: NodeMaintainerError = e.into();
                            tracing::debug!("Error in optional dependency script: {}", e);
                            return Ok(());
                        }
                        Err(e) => return Err(e.into()),
                    };
                    let stdout = script.stdout.take();
                    let stderr = script.stderr.take();
                    let stdout_name = name.clone();
                    let stderr_name = name.clone();
                    let stdout_on_line = opts.on_script_line.clone();
                    let stderr_on_line = opts.on_script_line.clone();
                    let stdout_span = span;
                    let stderr_span = stdout_span.clone();
                    let event_clone = event.clone();
                    let join = futures::try_join!(
                        async_std::task::spawn_blocking(move || {
                            let _enter = stdout_span.enter();
                            if let Some(stdout) = stdout {
                                for line in BufReader::new(stdout).lines() {
                                    let line = line?;
                                    tracing::debug!("stdout::{stdout_name}::{event}: {}", line);
                                    if let Some(on_script_line) = &stdout_on_line {
                                        on_script_line(&line);
                                    }
                                }
                            }
                            Ok::<_, NodeMaintainerError>(())
                        }),
                        async_std::task::spawn_blocking(move || {
                            let _enter = stderr_span.enter();
                            if let Some(stderr) = stderr {
                                for line in BufReader::new(stderr).lines() {
                                    let line = line?;
                                    tracing::debug!(
                                        "stderr::{stderr_name}::{event_clone}: {}",
                                        line
                                    );
                                    if let Some(on_script_line) = &stderr_on_line {
                                        on_script_line(&line);
                                    }
                                }
                            }
                            Ok::<_, NodeMaintainerError>(())
                        }),
                        async_std::task::spawn_blocking(move || {
                            script.wait()?;
                            Ok::<_, NodeMaintainerError>(())
                        }),
                    );
                    match join {
                        Ok(_) => {}
                        Err(e) if is_optional => {
                            tracing::debug!("Error in optional dependency script: {}", e);
                            return Ok(());
                        }
                        Err(e) => return Err(e),
                    }
                }

                Ok::<_, NodeMaintainerError>(())
            })
            .await?;
        tracing::debug!(
            "Ran lifecycle scripts for {event} in {}ms.",
            start.elapsed().as_millis()
        );
        Ok(())
    }

    // pub async fn run_scripts(&self, graph: &Graph, event: &str) -> Result<(), NodeMaintainerError> {
    //     let root = &self.opts.root;
    //     let pending = self
    //         .pending_rebuild
    //         .lock()
    //         .await
    //         .iter()
    //         .copied()
    //         .collect::<Vec<_>>();
    //     let remaining_dependencies = Arc::new(Mutex::new(
    //         pending
    //             .iter()
    //             .map(|idx| {
    //                 let mut deps = HashSet::new();
    //                 for dep in &pending {
    //                     if petgraph::algo::has_path_connecting(&graph.inner, *idx, *dep, None) {
    //                         deps.insert(*dep);
    //                     }
    //                 }
    //                 (*idx, deps)
    //             })
    //             .collect::<HashMap<NodeIndex, HashSet<NodeIndex>>>(),
    //     ));
    //     let dependents = Arc::new(
    //         pending
    //             .iter()
    //             .map(|idx| {
    //                 let mut deps = HashSet::new();
    //                 for dep in &pending {
    //                     if petgraph::algo::has_path_connecting(&graph.inner, *dep, *idx, None) {
    //                         deps.insert(*dep);
    //                     }
    //                 }
    //                 (*idx, deps)
    //             })
    //             .collect::<HashMap<NodeIndex, HashSet<NodeIndex>>>(),
    //     );
    //     receiver
    //         .map(Ok)
    //         .try_for_each_concurrent(
    //             self.opts.script_concurrency,
    //             move |(idx, remaining_dependencies, dependents)| async move {
    //                 let package_dir = if idx == graph.root {
    //                     root.clone()
    //                 } else {
    //                     let subdir = graph
    //                         .node_path(idx)
    //                         .iter()
    //                         .map(|x| x.to_string())
    //                         .collect::<Vec<_>>()
    //                         .join("/node_modules/");
    //                     root.join("node_modules").join(subdir)
    //                 };

    //                 let is_optional = graph.is_optional(idx);

    //                 let build_mani = BuildManifest::from_path(package_dir.join("package.json"))
    //                     .map_err(|e| {
    //                         NodeMaintainerError::BuildManifestReadError(
    //                             package_dir.join("package.json"),
    //                             e,
    //                         )
    //                     })?;

    //                 let name = graph[idx].package.name().to_string();
    //                 if build_mani.scripts.contains_key(event) {
    //                     let package_dir = package_dir.clone();
    //                     let root = root.clone();
    //                     let event = event.to_owned();
    //                     let event_clone = event.clone();
    //                     let span = tracing::info_span!("script");
    //                     let _span_enter = span.enter();
    //                     if let Some(on_script_start) = &self.opts.on_script_start {
    //                         on_script_start(&graph[idx].package, &event);
    //                     }
    //                     std::mem::drop(_span_enter);
    //                     let mut script = match async_std::task::spawn_blocking(move || {
    //                         OroScript::new(package_dir, event_clone)?
    //                             .workspace_path(root)
    //                             .spawn()
    //                     })
    //                     .await
    //                     {
    //                         Ok(script) => script,
    //                         Err(e) if is_optional => {
    //                             let e: NodeMaintainerError = e.into();
    //                             tracing::debug!("Error in optional dependency script: {}", e);
    //                             return Ok(());
    //                         }
    //                         Err(e) => return Err(e.into()),
    //                     };
    //                     let stdout = script.stdout.take();
    //                     let stderr = script.stderr.take();
    //                     let stdout_name = name.clone();
    //                     let stderr_name = name.clone();
    //                     let stdout_on_line = self.opts.on_script_line.clone();
    //                     let stderr_on_line = self.opts.on_script_line.clone();
    //                     let stdout_span = span;
    //                     let stderr_span = stdout_span.clone();
    //                     let event_clone = event.clone();
    //                     let join = futures::try_join!(
    //                         async_std::task::spawn_blocking(move || {
    //                             let _enter = stdout_span.enter();
    //                             if let Some(stdout) = stdout {
    //                                 for line in BufReader::new(stdout).lines() {
    //                                     let line = line?;
    //                                     tracing::debug!("stdout::{stdout_name}::{event}: {}", line);
    //                                     if let Some(on_script_line) = &stdout_on_line {
    //                                         on_script_line(&line);
    //                                     }
    //                                 }
    //                             }
    //                             Ok::<_, NodeMaintainerError>(())
    //                         }),
    //                         async_std::task::spawn_blocking(move || {
    //                             let _enter = stderr_span.enter();
    //                             if let Some(stderr) = stderr {
    //                                 for line in BufReader::new(stderr).lines() {
    //                                     let line = line?;
    //                                     tracing::debug!(
    //                                         "stderr::{stderr_name}::{event_clone}: {}",
    //                                         line
    //                                     );
    //                                     if let Some(on_script_line) = &stderr_on_line {
    //                                         on_script_line(&line);
    //                                     }
    //                                 }
    //                             }
    //                             Ok::<_, NodeMaintainerError>(())
    //                         }),
    //                         async_std::task::spawn_blocking(move || {
    //                             script.wait()?;
    //                             Ok::<_, NodeMaintainerError>(())
    //                         }),
    //                     );
    //                     match join {
    //                         Ok(_) => {}
    //                         Err(e) if is_optional => {
    //                             tracing::debug!("Error in optional dependency script: {}", e);
    //                             return Ok(());
    //                         }
    //                         Err(e) => return Err(e),
    //                     }
    //                 }

    //                 if let Some(set) = dependents.get(&idx) {
    //                     let mut remaining = remaining_dependencies.lock().await;
    //                     for dep in set {
    //                         if let Some(remaining) = remaining.get_mut(dep) {
    //                             remaining.remove(&idx);
    //                             if remaining.is_empty() {
    //                                 sender_ref.unbounded_send((
    //                                     *dep,
    //                                     remaining_dependencies.clone(),
    //                                     dependents.clone(),
    //                                 ))?;
    //                             }
    //                         }
    //                     }
    //                 }

    //                 Ok::<_, NodeMaintainerError>(())
    //             },
    //         )
    //         .await?;

    //     Ok(())
    // }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn supports_reflink(src_dir: &Path, dest_dir: &Path) -> bool {
    let temp = match tempfile::NamedTempFile::new_in(src_dir) {
        Ok(t) => t,
        Err(e) => {
            tracing::debug!("error creating tempfile while checking for reflink support: {e}.");
            return false;
        }
    };
    match std::fs::write(&temp, "a") {
        Ok(_) => {}
        Err(e) => {
            tracing::debug!("error writing to tempfile while checking for reflink support: {e}.");
            return false;
        }
    };
    let tempdir = match tempfile::TempDir::new_in(dest_dir) {
        Ok(t) => t,
        Err(e) => {
            tracing::debug!(
                "error creating destination tempdir while checking for reflink support: {e}."
            );
            return false;
        }
    };
    let supports_reflink = reflink::reflink(temp.path(), tempdir.path().join("b"))
        .map(|_| true)
        .map_err(|e| {
            tracing::debug!(
                "reflink support check failed. Files will be hard linked or copied. ({e})"
            );
            e
        })
        .unwrap_or(false);

    if supports_reflink {
        tracing::debug!("Verified reflink support. Extracted data will use copy-on-write reflinks instead of hard links or full copies.")
    }

    supports_reflink
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn link_bin(from: &Path, to: &Path) -> Result<(), NodeMaintainerError> {
    #[cfg(windows)]
    oro_shim_bin::shim_bin(from, to)?;
    #[cfg(not(windows))]
    {
        use std::os::unix::fs::PermissionsExt;
        let meta = from.metadata()?;
        let mut perms = meta.permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(from, perms)?;
        let relative = pathdiff::diff_paths(from, to.parent().unwrap()).unwrap();
        std::os::unix::fs::symlink(relative, to)?;
    }
    Ok(())
}
