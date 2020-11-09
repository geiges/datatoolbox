import networkx as nx
import pandas as pd
import matplotlib.pyplot as plt
from . import core

#

def shorten_find_output(dataframe):
    pd.set_option('display.max_rows', 10)
    pd.set_option('display.max_columns', 5)
    pd.set_option('display.width', 1000)
    return dataframe.reset_index(drop=True).drop(['scenario', 'model', 'category', 'entity', 'source_year', 'source_name', 'unit'], axis=1)

def get_data_trees(**kwargs):
    find = core.DB.getInventory
    results = find(**kwargs)
    return _process_query(results)
    

def _process_query(results):
    
    # Initialize graphs for different data heads
    heads, graphs = [], {}
    
    if len(results.scenario.unique()) > 1:
        raise ValueError(
            "Multiple scenarios were detected, ensure that the"
            + " scenario name/ model/ data source etc. specify a unique scenario"
        )
    if len(results.scenario.unique()) == 0:
        raise ValueError(
            "Specified kwargs point to an empty list of scenarios. "
            + "Change kwargs or update your database."
        )
    scenario = list(results.scenario.unique())[0]

    # Iterate through data inventory
    for ix in results.index:

        # Remove scenario and model name
        ix = ix.split("__")[0]
        nodes = ix.split("|")

        # If first time occurrence of data head, update graphs
        if nodes[0] not in heads:
            heads.append(nodes[0])
            graph = nx.DiGraph()
            attr = {"label": nodes[0]}
            graph.add_node(nodes[0], **attr)
            graphs.update({nodes[0]: graph})

        # Fetch correct graph/  dict/ list
        graph = graphs[nodes[0]]

        # Add branches to tree
        root = nodes[0]
        for i, name_short in enumerate(nodes[1:]):
            # Get unique node name
            name_long = "|".join(nodes[1 : i + 2])
            # Add node to graph if it does not exist already
            if name_long not in graph.nodes:
                # Mark with "*"" if it point to a data table
                label = name_short
                # Add a short label for better visualization
                attr = {"label": label}
                graph.add_node(name_long, **attr)
            if i == len(nodes[1:]) - 1:
                graph.nodes[name_long]["label"] = name_short + "*"
            # Add edge
            graph.add_edge(root, name_long)
            root = name_long

    return graphs, scenario


def get_positions(graph, x_offset=1):
    """Get positions of nodes for horizontally aligned tree visualization

    Args:
        graph (networkx DiGraph): directed graph which is a tree
        x_offset (int, optional): Offset between horizontal spacing. Defaults to 1.

    Raises:
        TypeError: if not networkx DiGraph or tree

    Returns:
        dict: dictionary, mapping nodes to xy positions
    """

    # Check if tree
    if not isinstance(graph, nx.DiGraph):
        raise TypeError("Has to be a networkx DiGraph")

    if not nx.is_tree(graph):
        raise TypeError("Has to be a tree")

    # Determine root node
    root = next(iter(nx.topological_sort(graph)))

    # Determine number of subbranches
    out_degrees = graph.out_degree()

    def nb_leafs_in_subtree(root):
        """Recursive function for getting the number of leafs attached to root (root inclusive)

        Args:
            root (networkx node): root of subtree

        Returns:
            int: number of leafs in subtree
        """
        if out_degrees[root] == 0:
            nb_children = 1
        else:
            nb_children = sum(
                nb_leafs_in_subtree(child) for child in graph.neighbors(root)
            )

        return nb_children

    def set_positions(
        root_, x_spacing={}, depth=0, pos={root: (0, nb_leafs_in_subtree(root) / 2)},
    ):
        """Sets positions of nodes in a tree for horizontally aligned tree in a recursive fashion

        Args:
            root_ (networkx node): root of subtree
            x_spacing (dict, optional): Dictionary for keeping track of required horizontal spacing. Defaults to {}.
            depth (int, optional): Current tree depth. Defaults to 0.
            pos (dict, optional): [description]. Defaults to {root: (0, nb_leafs_in_subtree(root) / 2)}.

        Returns:
            (dict, dict): Returns  x_spacing and pos
        """

        # Consider length of root for x-spacing
        x_spacing.setdefault(depth, len(graph.nodes[root_]["label"]))
        x_spacing[depth] = max(x_spacing[depth], len(graph.nodes[root_]["label"]))

        if out_degrees[root_] == 0:
            return

        # Distribute children of root_ across the y-axis
        offset = 0
        depth += 1
        x_spacing.setdefault(depth, 0)

        for child in graph.neighbors(root_):
            y_pos = (
                pos[root_][1]
                - nb_leafs_in_subtree(root_) / 2
                + nb_leafs_in_subtree(child) / 2
                + offset
            )
            pos.update({child: (depth, y_pos)})
            offset += nb_leafs_in_subtree(child)

            set_positions(child, x_spacing, depth=depth, pos=pos)

        return pos, x_spacing

    # Determine positions of nodes
    pos, x_spacing = set_positions(root)
    # Re-adjust x-spacing
    pos = {
        key: (sum(x_spacing[i] + x_offset for i in range(pos_[0])), pos_[1])
        for key, pos_ in pos.items()
    }

    return pos, x_spacing


def plot_tree(
    graph,
    scenario,
    x_offset=3,
    fontsize=12,
    figsize=None,
    savefig_path=None,
    dpi=100,
):
    """Plots a tree indicating available data of a scenario 

    Parameters
    ----------
    graph : networkx.DiGraph
        tree in digraph format (obtained via get_data_trees function)
    scenario : str
        scenario name
    x_offset : int, optional
        x offset between nodes, by default 3
    fontsize : int, optional
        fontsize of the node labels (either fontsize or figsize can be specified not
        both), by default 12
    figsize : 2-dim tuple or None, optional
        figure size (either fontsize or figsize can be specified not
        both), by default None
    savefig_path : str or None, optional
        path to save figure to (e.g savefig_path = os.path.join(os.getcwd(), "fig.png") ),
        by default None
    dpi : int, optional
        dots per inches used in savefig, by default 100
    """

    pos, x_spacing = get_positions(graph, x_offset=x_offset)

    if figsize is None:
        fig, ax = plt.subplots()
    else:
        fig, ax = plt.subplots(figsize=figsize)

    # Draw the graph
    nx.draw(graph, pos=pos, with_labels=False, ax=ax, node_color="none")

    # Set xlim and ylim
    x_max = (
        sum(
            x + x_offset
            for level, x in x_spacing.items()
            if type(level) == int
        )
        - x_offset
    )
    y_max = max(pos_[1] for pos_ in pos.values()) + 1
    ax.set_xlim([0, x_max]), ax.set_ylim([0, y_max])

    # Get fontsize or reset figsize to avoid overlaps
    if fontsize is None:
        x_fig, y_fig = fig.get_size_inches() * fig.dpi
        fontsize = min(x_fig / (x_max / 1.5), y_fig / (y_max * 2.5))
    else:
        x_fig = 2 + (fontsize * (x_max / 1.5) / fig.dpi)
        y_fig = 2 + (fontsize * y_max * 2.5 / fig.dpi)
        fig.set_size_inches(x_fig, y_fig, forward=True)

    # Add node labels
    for node, xy in pos.items():
        text = graph.nodes[node]["label"]
        ax.annotate(
            text,
            xy,
            bbox=dict(pad=0.2, fc="gainsboro", ec="k", boxstyle="round"),
            family="monospace",
            fontsize=fontsize,
            verticalalignment="center",
            horizontalalignment="left",
        )

    # Add legend
    ax.annotate(
        "*: data available\nscenario: {}".format(scenario),
        (x_max, y_max),
        family="monospace",
        fontsize=fontsize,
        verticalalignment="top",
        horizontalalignment="right",
    )

    # Plot and save
    plt.tight_layout()

    if savefig_path is not None:
        plt.savefig(savefig_path, dpi=dpi)

    plt.show()

def plot_query_as_graph(results, savefig_path=None):
    
   graphs, scenario =  _process_query(results)
   for gKey in graphs.keys():
       plot_tree(graphs[gKey], 
                 scenario, 
#                 figsize=[5,6],
                 savefig_path=savefig_path)

    