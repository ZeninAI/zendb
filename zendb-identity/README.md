# zendb-identity

Client-side identity and workspace admission contracts.

This crate owns device signing, OIDC claim validation adapters, workspace
credentials, invites, bootstrap envelopes, peer claims, local credential
storage, revocation checks, and admission interfaces. It does not implement a
central identity server. Hosted credential issuance is an optional outbound
adapter in `zendb-external`.

The stable `DeviceId` comes from `zendb-types` and is also the identity carried
by HLC and replication ranges. A device may hold credentials for several user
or guest principals without changing its device identity.
