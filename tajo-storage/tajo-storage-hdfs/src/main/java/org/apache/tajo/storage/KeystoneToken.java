package org.apache.tajo.storage;

import org.apache.tajo.json.GsonObject;
import org.apache.tajo.util.TUtil;

import java.net.URI;
import java.util.List;

public class KeystoneToken implements GsonObject {
  private Access access;

  @Override
  public String toJson() {
    return null;
  }

  public Access getAccess() {
    return access;
  }

  public void setAccess(Access access) {
    this.access = access;
  }

  public static class Access implements GsonObject {
    private Token token;
    private List<ServiceCatalog> serviceCatalog;
    private User user;
    private Metadata metadata;

    @Override
    public String toJson() {
      return null;
    }

    public Metadata getMetadata() {
      return metadata;
    }

    public void setMetadata(Metadata metadata) {
      this.metadata = metadata;
    }

    public User getUser() {
      return user;
    }

    public void setUser(User user) {
      this.user = user;
    }

    public Token getToken() {
      return token;
    }

    public void setToken(Token token) {
      this.token = token;
    }

    public List<ServiceCatalog> getServiceCatalog() {
      return serviceCatalog;
    }

    public void setServiceCatalog(List<ServiceCatalog> serviceCatalog) {
      this.serviceCatalog = serviceCatalog;
    }
  }

  public static class Token implements GsonObject {
    private String issued_at;
    private String expires;
    private String id;
    private String audit_ids;
    private Tenant tenant;

    @Override
    public String toJson() {
      return null;
    }

    public String getIssued_at() {
      return issued_at;
    }

    public void setIssued_at(String issued_at) {
      this.issued_at = issued_at;
    }

    public String getExpires() {
      return expires;
    }

    public void setExpires(String expires) {
      this.expires = expires;
    }

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public String getAudit_ids() {
      return audit_ids;
    }

    public void setAudit_ids(String audit_ids) {
      this.audit_ids = audit_ids;
    }

    public Tenant getTenant() {
      return tenant;
    }

    public void setTenant(Tenant tenant) {
      this.tenant = tenant;
    }
  }

  public static class Tenant implements GsonObject {
    private String description;
    private boolean enabled;
    private String id;
    private String parent_id;
    private String name;

    @Override
    public String toJson() {
      return null;
    }

    public String getDescription() {
      return description;
    }

    public void setDescription(String description) {
      this.description = description;
    }

    public boolean isEnabled() {
      return enabled;
    }

    public void setEnabled(boolean enabled) {
      this.enabled = enabled;
    }

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public String getParent_id() {
      return parent_id;
    }

    public void setParent_id(String parent_id) {
      this.parent_id = parent_id;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }
  }

  public static class ServiceCatalog implements GsonObject {
    private List<Endpoint> endpoints = TUtil.newList();
    private List<String> endpoints_links = TUtil.newList();
    private String type;
    private String name;

    @Override
    public String toJson() {
      return null;
    }

    public List<Endpoint> getEndpoints() {
      return endpoints;
    }

    public void setEndpoints(List<Endpoint> endpoints) {
      this.endpoints = endpoints;
    }

    public List<String> getEndpoints_links() {
      return endpoints_links;
    }

    public void setEndpoints_links(List<String> endpoints_links) {
      this.endpoints_links = endpoints_links;
    }

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }
  }

  public static class Endpoint implements GsonObject {
    private String id;
    private URI adminURL;
    private URI internalURL;
    private URI publicURL;
    private String region;

    @Override
    public String toJson() {
      return null;
    }

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public URI getAdminURL() {
      return adminURL;
    }

    public void setAdminURL(URI adminURL) {
      this.adminURL = adminURL;
    }

    public URI getInternalURL() {
      return internalURL;
    }

    public void setInternalURL(URI internalURL) {
      this.internalURL = internalURL;
    }

    public URI getPublicURL() {
      return publicURL;
    }

    public void setPublicURL(URI publicURL) {
      this.publicURL = publicURL;
    }

    public String getRegion() {
      return region;
    }

    public void setRegion(String region) {
      this.region = region;
    }
  }

  public static class User implements GsonObject {
    private String username;
    private List<String> roles_links = TUtil.newList();
    private String id;
    private List<Role> roles = TUtil.newList();
    private String name;

    @Override
    public String toJson() {
      return null;
    }

    public String getUsername() {
      return username;
    }

    public void setUsername(String username) {
      this.username = username;
    }

    public List<String> getRoles_links() {
      return roles_links;
    }

    public void setRoles_links(List<String> roles_links) {
      this.roles_links = roles_links;
    }

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public List<Role> getRoles() {
      return roles;
    }

    public void setRoles(List<Role> roles) {
      this.roles = roles;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }
  }

  public static class Role implements GsonObject {
    private String name;

    public void setName(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    @Override
    public String toJson() {
      return null;
    }
  }

  public static class Metadata implements GsonObject {
    private int is_admin;
    private List<String> roles = TUtil.newList();

    @Override
    public String toJson() {
      return null;
    }
  }
}
