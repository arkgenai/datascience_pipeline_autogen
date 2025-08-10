-- AGE Graph Schema Definition
-- Apache AGE (A Graph Extension for PostgreSQL)

-- Load AGE extension
LOAD 'age';
SET search_path = ag_catalog, "$user", public;

-- Create graph
SELECT create_graph('business_graph');

-- =======================
-- VERTEX LABELS CREATION
-- =======================

-- Purchase Request vertex
SELECT create_vlabel('business_graph', 'pr');

-- Purchase Order vertex  
SELECT create_vlabel('business_graph', 'po');

-- Invoice vertex
SELECT create_vlabel('business_graph', 'inv');

-- Master Service Agreement vertex
SELECT create_vlabel('business_graph', 'msa');

-- Amendment vertex
SELECT create_vlabel('business_graph', 'amd');

-- =======================
-- EDGE LABELS CREATION
-- =======================

-- Fulfills relationship (pr -> po, po -> inv, etc.)
SELECT create_elabel('business_graph', 'fulfills');

-- Party relationship (entities involved in agreements)
SELECT create_elabel('business_graph', 'party');

-- Parent relationship (hierarchical relationships, amendments to MSA)
SELECT create_elabel('business_graph', 'parent');

-- =======================
-- INDEXING STRATEGY (CREATE BEFORE DATA INSERTION)
-- =======================

-- Vertex Property Indices
-- Creating indices BEFORE data insertion for better performance

-- Index on PR properties
SELECT create_property_index('business_graph', 'pr', 'id');
SELECT create_property_index('business_graph', 'pr', 'department');
SELECT create_property_index('business_graph', 'pr', 'status');
SELECT create_property_index('business_graph', 'pr', 'amount');

-- Index on PO properties  
SELECT create_property_index('business_graph', 'po', 'id');
SELECT create_property_index('business_graph', 'po', 'vendor');
SELECT create_property_index('business_graph', 'po', 'status');
SELECT create_property_index('business_graph', 'po', 'amount');

-- Index on Invoice properties
SELECT create_property_index('business_graph', 'inv', 'id');
SELECT create_property_index('business_graph', 'inv', 'vendor');
SELECT create_property_index('business_graph', 'inv', 'status');
SELECT create_property_index('business_graph', 'inv', 'amount');

-- Index on MSA properties
SELECT create_property_index('business_graph', 'msa', 'id');
SELECT create_property_index('business_graph', 'msa', 'vendor');
SELECT create_property_index('business_graph', 'msa', 'start_date');
SELECT create_property_index('business_graph', 'msa', 'end_date');

-- Index on Amendment properties
SELECT create_property_index('business_graph', 'amd', 'id');
SELECT create_property_index('business_graph', 'amd', 'type');
SELECT create_property_index('business_graph', 'amd', 'effective_date');

-- Edge Property Indices
SELECT create_property_index('business_graph', 'fulfills', 'created_date');
SELECT create_property_index('business_graph', 'fulfills', 'process_time');
SELECT create_property_index('business_graph', 'party', 'role');
SELECT create_property_index('business_graph', 'party', 'relationship_type');
SELECT create_property_index('business_graph', 'parent', 'amendment_type');
SELECT create_property_index('business_graph', 'parent', 'approval_date');

-- =======================
-- SAMPLE DATA INSERTION
-- =======================

-- Insert Purchase Requests
SELECT * FROM cypher('business_graph', $
    CREATE (pr1:pr {id: 'PR001', amount: 5000, department: 'IT', status: 'approved', created_date: '2024-01-15'})
$) AS (result agtype);

SELECT * FROM cypher('business_graph', $$
    CREATE (pr2:pr {id: 'PR002', amount: 12000, department: 'Marketing', status: 'pending', created_date: '2024-02-01'})
$$) AS (result agtype);

-- Insert Purchase Orders
SELECT * FROM cypher('business_graph', $$
    CREATE (po1:po {id: 'PO001', amount: 5000, vendor: 'TechCorp', status: 'issued', issue_date: '2024-01-20'})
$$) AS (result agtype);

-- Insert Invoices
SELECT * FROM cypher('business_graph', $$
    CREATE (inv1:inv {id: 'INV001', amount: 5000, vendor: 'TechCorp', status: 'paid', invoice_date: '2024-01-25'})
$$) AS (result agtype);

-- Insert Master Service Agreements
SELECT * FROM cypher('business_graph', $$
    CREATE (msa1:msa {id: 'MSA001', vendor: 'TechCorp', start_date: '2024-01-01', end_date: '2024-12-31', value: 100000})
$$) AS (result agtype);

-- Insert Amendments
SELECT * FROM cypher('business_graph', $$
    CREATE (amd1:amd {id: 'AMD001', type: 'value_increase', amount: 25000, effective_date: '2024-06-01'})
$$) AS (result agtype);

-- =======================
-- RELATIONSHIPS CREATION
-- =======================

-- PR fulfills relationship to PO
SELECT * FROM cypher('business_graph', $$
    MATCH (pr:pr {id: 'PR001'}), (po:po {id: 'PO001'})
    CREATE (pr)-[:fulfills {created_date: '2024-01-20', process_time: 5}]->(po)
$$) AS (result agtype);

-- PO fulfills relationship to Invoice
SELECT * FROM cypher('business_graph', $$
    MATCH (po:po {id: 'PO001'}), (inv:inv {id: 'INV001'})
    CREATE (po)-[:fulfills {created_date: '2024-01-25', delivery_confirmed: true}]->(inv)
$$) AS (result agtype);

-- Party relationships
SELECT * FROM cypher('business_graph', $$
    MATCH (po:po {id: 'PO001'}), (msa:msa {id: 'MSA001'})
    CREATE (po)-[:party {role: 'governed_by', relationship_type: 'contract'}]->(msa)
$$) AS (result agtype);

-- Parent relationship (Amendment to MSA)
SELECT * FROM cypher('business_graph', $$
    MATCH (amd:amd {id: 'AMD001'}), (msa:msa {id: 'MSA001'})
    CREATE (amd)-[:parent {amendment_type: 'modification', approval_date: '2024-05-15'}]->(msa)
$$) AS (result agtype);

-- Indices are now created above, before data insertion

-- =======================
-- COMPOSITE INDICES
-- =======================

-- Composite indices for common query patterns
-- Note: AGE may have limitations on composite indices, check version compatibility

-- Vendor + Status composite for PO and Invoice lookups
-- SELECT create_property_index('business_graph', 'po', ['vendor', 'status']);
-- SELECT create_property_index('business_graph', 'inv', ['vendor', 'status']);

-- =======================
-- QUERY EXAMPLES
-- =======================

-- Find all Purchase Orders that fulfill Purchase Requests
SELECT * FROM cypher('business_graph', $$
    MATCH (pr:pr)-[:fulfills]->(po:po)
    RETURN pr.id AS purchase_request, po.id AS purchase_order, pr.amount, po.vendor
$$) AS (pr_id agtype, po_id agtype, amount agtype, vendor agtype);

-- Find complete fulfillment chain: PR -> PO -> Invoice
SELECT * FROM cypher('business_graph', $$
    MATCH (pr:pr)-[:fulfills]->(po:po)-[:fulfills]->(inv:inv)
    RETURN pr.id AS purchase_request, po.id AS purchase_order, inv.id AS invoice, 
           pr.amount AS pr_amount, inv.amount AS inv_amount
$$) AS (pr_id agtype, po_id agtype, inv_id agtype, pr_amount agtype, inv_amount agtype);

-- Find all amendments to MSAs
SELECT * FROM cypher('business_graph', $$
    MATCH (amd:amd)-[:parent]->(msa:msa)
    RETURN amd.id AS amendment, msa.id AS master_agreement, 
           amd.type AS amendment_type, amd.amount AS change_amount
$$) AS (amd_id agtype, msa_id agtype, amd_type agtype, change_amount agtype);

-- Find all Purchase Orders governed by MSAs
SELECT * FROM cypher('business_graph', $$
    MATCH (po:po)-[:party]->(msa:msa)
    RETURN po.id AS purchase_order, msa.id AS master_agreement, 
           po.vendor, msa.start_date, msa.end_date
$$) AS (po_id agtype, msa_id agtype, vendor agtype, start_date agtype, end_date agtype);

-- =======================
-- INDEX MONITORING
-- =======================

-- Query to check existing indices
SELECT * FROM ag_catalog.ag_label WHERE graph = 'business_graph';

-- =======================
-- PERFORMANCE TIPS
-- =======================

/*
1. Property Indices:
   - Create indices on frequently queried properties
   - Consider cardinality - high cardinality properties benefit most from indexing
   - Monitor query performance and add indices as needed

2. Edge Direction:
   - AGE traverses edges more efficiently in their natural direction
   - Consider edge direction when designing relationships

3. Query Optimization:
   - Use MATCH patterns that leverage indices
   - Filter early in query execution
   - Limit result sets where possible

4. Maintenance:
   - Regularly analyze query performance
   - Monitor index usage statistics
   - Drop unused indices to save storage space
*/