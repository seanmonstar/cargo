#![allow(unknown_lints)]
#![allow(clippy::identity_op)] // used for vertical alignment

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, Cursor, Read};

use failure::bail;
use reqwest::{Client, Method, RequestBuilder, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json;
use url::percent_encoding::{percent_encode, QUERY_ENCODE_SET};

pub type Result<T> = std::result::Result<T, failure::Error>;
pub type Tarball = Box<dyn Read + Send + 'static>;

pub struct Registry {
    /// The base URL for issuing API requests.
    host: String,
    /// Optional authorization token.
    /// If None, commands requiring authorization will fail.
    token: Option<String>,
    /// Reqwest client for issuing requests.
    client: Client,
}

#[derive(PartialEq, Clone, Copy)]
pub enum Auth {
    Authorized,
    Unauthorized,
}

#[derive(Deserialize)]
pub struct Crate {
    pub name: String,
    pub description: Option<String>,
    pub max_version: String,
}

#[derive(Serialize)]
pub struct NewCrate {
    pub name: String,
    pub vers: String,
    pub deps: Vec<NewCrateDependency>,
    pub features: BTreeMap<String, Vec<String>>,
    pub authors: Vec<String>,
    pub description: Option<String>,
    pub documentation: Option<String>,
    pub homepage: Option<String>,
    pub readme: Option<String>,
    pub readme_file: Option<String>,
    pub keywords: Vec<String>,
    pub categories: Vec<String>,
    pub license: Option<String>,
    pub license_file: Option<String>,
    pub repository: Option<String>,
    pub badges: BTreeMap<String, BTreeMap<String, String>>,
    #[serde(default)]
    pub links: Option<String>,
}

#[derive(Serialize)]
pub struct NewCrateDependency {
    pub optional: bool,
    pub default_features: bool,
    pub name: String,
    pub features: Vec<String>,
    pub version_req: String,
    pub target: Option<String>,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub registry: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub explicit_name_in_toml: Option<String>,
}

#[derive(Deserialize)]
pub struct User {
    pub id: u32,
    pub login: String,
    pub avatar: Option<String>,
    pub email: Option<String>,
    pub name: Option<String>,
}

pub struct Warnings {
    pub invalid_categories: Vec<String>,
    pub invalid_badges: Vec<String>,
    pub other: Vec<String>,
}

#[derive(Deserialize)]
struct R {
    ok: bool,
}
#[derive(Deserialize)]
struct OwnerResponse {
    ok: bool,
    msg: String,
}
#[derive(Deserialize)]
struct ApiErrorList {
    errors: Vec<ApiError>,
}
#[derive(Deserialize)]
struct ApiError {
    detail: String,
}
#[derive(Serialize)]
struct OwnersReq<'a> {
    users: &'a [&'a str],
}
#[derive(Deserialize)]
struct Users {
    users: Vec<User>,
}
#[derive(Deserialize)]
struct TotalCrates {
    total: u32,
}
#[derive(Deserialize)]
struct Crates {
    crates: Vec<Crate>,
    meta: TotalCrates,
}
impl Registry {
    pub fn new(host: String, token: Option<String>) -> Registry {
        Registry::new_handle(host, token, Client::new())
    }

    pub fn new_handle(host: String, token: Option<String>, client: Client) -> Registry {
        Registry {
            host,
            token,
            client,
        }
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn add_owners(&mut self, krate: &str, owners: &[&str]) -> Result<String> {
        let body = serde_json::to_string(&OwnersReq { users: owners })?;
        let body = self.put(&format!("/crates/{}/owners", krate), body.as_bytes())?;
        assert!(serde_json::from_str::<OwnerResponse>(&body)?.ok);
        Ok(serde_json::from_str::<OwnerResponse>(&body)?.msg)
    }

    pub fn remove_owners(&mut self, krate: &str, owners: &[&str]) -> Result<()> {
        let body = serde_json::to_string(&OwnersReq { users: owners })?;
        let body = self.delete(&format!("/crates/{}/owners", krate), Some(body.as_bytes()))?;
        assert!(serde_json::from_str::<OwnerResponse>(&body)?.ok);
        Ok(())
    }

    pub fn list_owners(&mut self, krate: &str) -> Result<Vec<User>> {
        let body = self.get(&format!("/crates/{}/owners", krate))?;
        Ok(serde_json::from_str::<Users>(&body)?.users)
    }

    pub fn publish(&mut self, krate: &NewCrate, tarball: Tarball, tarball_size: u64) -> Result<Warnings> {
        let json = serde_json::to_string(krate)?;
        // Prepare the body. The format of the upload request is:
        //
        //      <le u32 of json>
        //      <json request> (metadata for the package)
        //      <le u32 of tarball>
        //      <source tarball>
        let header = {
            let mut w = Vec::new();
            w.extend(
                [
                    (json.len() >> 0) as u8,
                    (json.len() >> 8) as u8,
                    (json.len() >> 16) as u8,
                    (json.len() >> 24) as u8,
                ].iter().cloned(),
            );
            w.extend(json.as_bytes().iter().cloned());
            w.extend(
                [
                    (tarball_size >> 0) as u8,
                    (tarball_size >> 8) as u8,
                    (tarball_size >> 16) as u8,
                    (tarball_size >> 24) as u8,
                ].iter().cloned(),
            );
            w
        };
        let size =  tarball_size + header.len() as u64;
        let mut req_body = Cursor::new(header).chain(tarball);

        let url = format!("{}/api/v1/crates/new", self.host);
        let token = match self.token.as_ref() {
            Some(s) => s,
            None => bail!("no upload token found, please run `cargo login`"),
        };

        let response = if url.starts_with("file://") {
            // reqwest doesn't handle file:// URLs, so do it ourselves.
            let mut file = File::create(&url["file://".len()..])?;
            io::copy(&mut req_body, &mut file)?;
            "{}".parse()?
        } else {
            let req_body = reqwest::Body::sized(req_body, size as u64);

            let req = self.client
                .put(&url)
                .header("accept", "application/json")
                .header("authorization", &**token)
                .body(req_body);

            let res_body = send(req)?;

            if res_body.is_empty() {
                "{}".parse()?
            } else {
                res_body.parse::<serde_json::Value>()?
            }
        };


        let invalid_categories: Vec<String> = response
            .get("warnings")
            .and_then(|j| j.get("invalid_categories"))
            .and_then(|j| j.as_array())
            .map(|x| x.iter().flat_map(|j| j.as_str()).map(Into::into).collect())
            .unwrap_or_else(Vec::new);

        let invalid_badges: Vec<String> = response
            .get("warnings")
            .and_then(|j| j.get("invalid_badges"))
            .and_then(|j| j.as_array())
            .map(|x| x.iter().flat_map(|j| j.as_str()).map(Into::into).collect())
            .unwrap_or_else(Vec::new);

        let other: Vec<String> = response
            .get("warnings")
            .and_then(|j| j.get("other"))
            .and_then(|j| j.as_array())
            .map(|x| x.iter().flat_map(|j| j.as_str()).map(Into::into).collect())
            .unwrap_or_else(Vec::new);

        Ok(Warnings {
            invalid_categories,
            invalid_badges,
            other,
        })
    }

    pub fn search(&mut self, query: &str, limit: u32) -> Result<(Vec<Crate>, u32)> {
        let formatted_query = percent_encode(query.as_bytes(), QUERY_ENCODE_SET);
        let body = self.req(
            Method::GET,
            &format!("/crates?q={}&per_page={}", formatted_query, limit),
            None,
            Auth::Unauthorized,
        )?;

        let crates = serde_json::from_str::<Crates>(&body)?;
        Ok((crates.crates, crates.meta.total))
    }

    pub fn yank(&mut self, krate: &str, version: &str) -> Result<()> {
        let body = self.delete(&format!("/crates/{}/{}/yank", krate, version), None)?;
        assert!(serde_json::from_str::<R>(&body)?.ok);
        Ok(())
    }

    pub fn unyank(&mut self, krate: &str, version: &str) -> Result<()> {
        let body = self.put(&format!("/crates/{}/{}/unyank", krate, version), &[])?;
        assert!(serde_json::from_str::<R>(&body)?.ok);
        Ok(())
    }

    fn put(&mut self, path: &str, b: &[u8]) -> Result<String> {
        self.req(Method::PUT, path, Some(b), Auth::Authorized)
    }

    fn get(&mut self, path: &str) -> Result<String> {
        self.req(Method::GET, path, None, Auth::Authorized)
    }

    fn delete(&mut self, path: &str, b: Option<&[u8]>) -> Result<String> {
        self.req(Method::DELETE, path, b, Auth::Authorized)
    }

    fn req(&mut self, method: Method, path: &str, body: Option<&[u8]>, authorized: Auth) -> Result<String> {
        let mut req = self.client
            .request(method, &format!("{}/api/v1{}", self.host, path))
            .header("accept", "application/json")
            .header("content-type", "application/json");

        if authorized == Auth::Authorized {
            let token = match self.token.as_ref() {
                Some(s) => s,
                None => bail!("no upload token found, please run `cargo login`"),
            };
            req = req.header("authorization", &**token);
        }

        match body {
            Some(body) => {
                send(req.body(body.to_vec()))
            }
            None => send(req)
        }
    }
}

fn send(req: RequestBuilder) -> Result<String> {

    let mut res = req.send()?;

    let body = match res.text() {
        Ok(body) => body,
        Err(..) => bail!("response body was not valid utf-8"),
    };

    match res.status() {
        StatusCode::OK => {}
        StatusCode::UNAUTHORIZED => bail!("received 403 unauthorized response code"),
        StatusCode::NOT_FOUND => bail!("received 404 not found response code"),
        code => bail!(
            "failed to get a 200 OK response, got {}\n\
             headers:\n\
             \t{:#?}\n\
             body:\n\
             {}",
            code,
            res.headers(),
            body
        ),
    }

    if let Ok(errors) = serde_json::from_str::<ApiErrorList>(&body) {
        let errors = errors.errors.into_iter().map(|s| s.detail);
        bail!("api errors: {}", errors.collect::<Vec<_>>().join(", "));
    }
    Ok(body)
}
