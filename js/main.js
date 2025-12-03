const BACKEND_URL = "https://gedcom-knowledge-graph-backend.onrender.com";

const uploadBtn = document.getElementById("uploadBtn");
const fileInput = document.getElementById("gedcomFile");
const status = document.getElementById("status");
const countDiv = document.getElementById("count");
const dl = document.getElementById("dl");
const svg = d3.select("#graph");

uploadBtn.addEventListener("click", uploadGedcom);

async function uploadGedcom() {
    status.textContent = "";
    if (!fileInput.files.length) {
    alert("Please choose a GEDCOM file first.");
    return;
    }
    const file = fileInput.files[0];
    const fd = new FormData();
    fd.append("file", file);

    try {
    status.textContent = "Uploading...";
    const resp = await fetch(`${BACKEND_URL}/parse`, { method: "POST", body: fd });

    if (!resp.ok) {
        const txt = await resp.text();
        alert("Upload failed: " + txt);
        status.textContent = "";
        return;
    }

    const data = await resp.json();
    status.textContent = "Done.";

    // show counts
    countDiv.textContent = `Persons: ${data.count.persons}  |  Families: ${data.count.families}`;

    // enable TTL download (create a blob)
    dl.style.display = "inline";
    const ttl_blob = new Blob([data.ttl], { type: "text/turtle;charset=utf-8" });
    const ttl_url = URL.createObjectURL(ttl_blob);
    dl.href = ttl_url;
    dl.download = "output.ttl";
    dl.textContent = "Download TTL";

    // render graph from returned graph JSON
    renderGraph(data.graph.nodes, data.graph.links);

    } catch (err) {
    console.error(err);
    alert("Upload failed: " + err);
    status.textContent = "";
    }
}

function renderGraph(nodes, links) {
    // clear previous
    svg.selectAll("*").remove();

    const width = +svg.attr("width");
    const height = +svg.attr("height");

    // tooltip DOM element (HTML)
    const tooltip = document.getElementById("tooltip");

    // ---- Add zoom rect FIRST (under the container) ----
    svg.append("rect")
    .attr("class", "zoom-rect")
    .attr("width", width)
    .attr("height", height)
    .style("fill", "transparent")
    .style("cursor", "move");

    // Group that will be transformed by zoom; sits above zoom-rect
    const container = svg.append("g").attr("class", "container");

    // Map ids -> node objects and fix link endpoints
    const nodeById = new Map(nodes.map(d => [d.id, d]));
    links.forEach(l => {
    l.source = nodeById.get(l.source);
    l.target = nodeById.get(l.target);
    });

    // Force simulation
    const simulation = d3.forceSimulation(nodes)
    .force("link", d3.forceLink(links).id(d => d.id).distance(90).strength(1))
    .force("charge", d3.forceManyBody().strength(-300))
    .force("center", d3.forceCenter(width / 2, height / 2))
    .on("tick", ticked);

    // Draw links
    const link = container.append("g").attr("class", "links")
    .selectAll("line")
    .data(links)
    .enter().append("line")
    .attr("class", "link")
    .attr("stroke", "#999")
    .attr("stroke-opacity", 0.6)
    .attr("stroke-width", 1.2);

    // Helper: initials
    function initialsOf(label) {
    if (!label) return "";
    const words = label.toString().trim().split(/\s+/).filter(Boolean);
    if (words.length === 0) return "";
    if (words.length === 1) {
        return words[0].slice(0, 2).toUpperCase();
    }
    return (words[0][0] + (words[1] ? words[1][0] : "")).toUpperCase();
    }

    // Node groups
    const node = container.append("g").attr("class", "nodes")
    .selectAll("g")
    .data(nodes)
    .enter().append("g")
    .attr("class", d => `node ${d.type}`)
    .style("cursor", "pointer");

    // For each node: measure initials text, draw rect, draw initials text
    const paddingX = 8;
    const paddingY = 6;
    node.each(function(d) {
    const g = d3.select(this);
    const fullLabel = (d.label || d.id).toString();
    const init = initialsOf(fullLabel) || "";

    // Append a hidden measuring text to compute width/height
    const measuring = g.append("text")
        .attr("class", "node-initial measuring")
        .attr("font-size", 12)
        .attr("text-anchor", "middle")
        .attr("dominant-baseline", "middle")
        .style("visibility", "hidden")
        .text(init);

    // Force browser to compute layout, then measure
    // (getBBox works once node is in DOM)
    const bbox = measuring.node().getBBox();
    const w = bbox.width + paddingX * 2;
    const h = bbox.height + paddingY * 2;

    // remove measuring text
    measuring.remove();

    // append rounded rect centered on (0,0)
    g.append("rect")
        .attr("class", "node-rect")
        .attr("rx", Math.min(12, h / 2))
        .attr("ry", Math.min(12, h / 2))
        .attr("x", -w / 2)
        .attr("y", -h / 2)
        .attr("width", w)
        .attr("height", h);

    // initials text (visible)
    g.append("text")
        .attr("class", "node-initial")
        .attr("text-anchor", "middle")
        .attr("dominant-baseline", "middle")
        .attr("font-size", 12)
        .attr("fill", "white")
        .style("pointer-events", "none") // so dragging works on the group
        .text(init);
    });

    // Add <title> fallback tooltip
    node.append("title").text(d => (d.label || d.id));

    // DRAG behavior (works with zoom)
    const drag = d3.drag()
    .on("start", (event, d) => {
        if (!event.active) simulation.alphaTarget(0.3).restart();
        // freeze simulation coords
        d.fx = d.x;
        d.fy = d.y;
    })
    .on("drag", (event, d) => {
        // event.x / event.y are in SVG coordinates (already transformed by zoom),
        // but because we apply transforms to container, d.x/d.y are simulation coords.
        // We keep it simple: update fx/fy with event coords translated back using inverse transform.
        const transform = d3.zoomTransform(svg.node());
        // convert screen coords to simulation coords by inverting current transform
        d.fx = transform.invertX(event.x);
        d.fy = transform.invertY(event.y);
    })
    .on("end", (event, d) => {
        if (!event.active) simulation.alphaTarget(0);
        // Release fixed position so nodes can move again
        d.fx = null;
        d.fy = null;
    });

    node.call(drag);

    // HTML tooltip interactions (hover)
    node.on("mouseover", (event, d) => {
    const text = (d.label || d.id).toString();
    if (tooltip) {
        tooltip.style.display = "block";
        tooltip.textContent = text;
        positionTooltip(event);
    }
    }).on("mousemove", (event) => {
    if (tooltip) positionTooltip(event);
    }).on("mouseout", () => {
    if (tooltip) tooltip.style.display = "none";
    });

    function positionTooltip(event) {
    const offsetX = 12;
    const offsetY = -12;
    tooltip.style.left = (event.pageX + offsetX) + "px";
    tooltip.style.top = (event.pageY + offsetY) + "px";
    }

    // Zoom behavior (apply transforms to container)
    const zoom = d3.zoom()
    .scaleExtent([0.1, 4])
    .on("zoom", (event) => {
        container.attr("transform", event.transform);
    });

    svg.call(zoom);

    // Tick function: update positions
    function ticked() {
    link
        .attr("x1", d => d.source.x)
        .attr("y1", d => d.source.y)
        .attr("x2", d => d.target.x)
        .attr("y2", d => d.target.y);

    node.attr("transform", d => `translate(${d.x},${d.y})`);
    }

    // Kick the simulation
    simulation.alpha(1).restart();
}
