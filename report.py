import plotly 
import plotly.plotly as py
import igraph as ig
from plotly.graph_objs import *
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot

def openFile(destinationPath, fileName):
    return open('{0}/{1}'.format(destinationPath, fileName), "r")

def graphAnalysis(destinationPath, verticesFile, edgesFile, headerV=True, headerE=True):
    
    g = ig.Graph()
    g.vs['httpRequestDomain'] = []
    g.vs['urlReferrer'] = []
    g.es['urlReferrer'] = []

    with openFile(destinationPath, verticesFile) as vertices:
        #vsCount=0
        for line in vertices:
            #vsCount += 1
            #if vsCount >=1000 : break
            if headerV:
                headerV = False
                continue
            tmp = line.split(',')
            gIndexV = int(tmp[0].strip())
            vID=int(tmp[1].strip())
            g.add_vertex(vID)            
            g.vs[gIndexV]['httpRequestDomain'] = tmp[2].strip()
            g.vs[gIndexV]['urlReferrer'] = tmp[3].strip()
    vertices.close()


    threshold=60
    with openFile(destinationPath, edgesFile) as edges:
        #esCount=0
        gIndexE=0
        for line in edges:
            #esCount += 1
            #if esCount >= 11000 : break
            if headerE:
                headerE = False
                continue
            tmp = line.split(',')
            vIDS=int(tmp[1].strip()) - 1
            vIDT=int(tmp[2].strip()) - 1
            if g.get_eid(vIDS, vIDT, directed=False, error=False) == -1:
                dvS = g.degree(vIDS)
                dvT = g.degree(vIDT)
                if(dvS<threshold and dvT<threshold):
                    g.add_edge(vIDS,vIDT)
                    g.es[gIndexE]['urlReferrer'] = tmp[3].strip()
                    print('{0}, {1}, {2}'.format(g.es[gIndexE], dvS+1, dvT+1))
                    gIndexE += 1
    edges.close()
    return g

def normalizeGraph(graphName):
    for vertex in graphName.vs():
        if(graphName.degree(vertex)<5):
            graphName.delete_vertices(vertex)
    return graphName



def drawGraph3D(graphData, online):
    
    gTitle='<b>{}</b>'.format(graphData[0][0])
    exportName=graphData[0][1]
    destinationPath=graphData[0][2]
    gTitle = '<br />'.join(gTitle.split('\n'))


    graphName = normalizeGraph(graphData[1])

    layt=graphName.layout('kk', dim=3)
    #layt=graphName.layout('fr3d', dim=3)
    N=len(graphName.vs())
    Edges=graphName.get_edgelist()
    vlabels=graphName.vs['httpRequestDomain']
    eLabels=graphName.es['urlReferrer']
    groupColor=graphName.degree()
    
    Xn=[layt[k][0] for k in range(N)]# x-coordinates of nodes
    Yn=[layt[k][1] for k in range(N)]# y-coordinates
    Zn=[layt[k][2] for k in range(N)]# z-coordinates
    Xe=[]
    Ye=[]
    Ze=[]
    for e in Edges:
        Xe+=[layt[e[0]][0],layt[e[1]][0], None]# x-coordinates of edge ends
        Ye+=[layt[e[0]][1],layt[e[1]][1], None]
        Ze+=[layt[e[0]][2],layt[e[1]][2], None]
    
    trace1=Scatter3d(x=Xe, 
                     y=Ye, 
                     z=Ze,
                     name='urlReferrer',
                     text=eLabels,
                     mode='lines',
                     line=Line(color='rgb(125,125,125)', width=1),
                     hoverinfo='none')

    trace2=Scatter3d(x=Xn, 
                     y=Yn, 
                     z=Zn, 
                     name='httpRequestDomainName',
                     text=vlabels,
                     mode='markers', 
                     hoverinfo='text',
                     marker=Marker(symbol='dot', 
                                   size='6', 
                                   color=groupColor, 
                                   colorscale='Viridis',
                                   showscale=True,
                                   colorbar=dict(titleside='right', title='Vertex Degree'),
                                   line=Line(color='rgb(50,50,50)', width=0.25)))



    axis=dict(showbackground=False, showline=False, zeroline=False, showgrid=False, showticklabels=False, showspikes=False, showaxeslabels=False, 
              showexponent=False, title = '')
    
    layout = Layout(title=gTitle,
                    showlegend=False,
                    scene=Scene(xaxis=XAxis(axis), yaxis=YAxis(axis),zaxis=ZAxis(axis), aspectmode='auto'),
                    hovermode='closest',
                    annotations=Annotations([Annotation(showarrow=True, 
                                                        x=0, 
                                                        y=0.1, 
                                                        xanchor='left', 
                                                        yanchor='bottom', 
                                                        font=Font(size=32))]),)

    data=Data([trace1, trace2])
    
    print('Draw the Graph.....')
    fig=Figure(data=data, layout=layout)
    
    print('Open the Graph.....')
    plot(fig, filename='{0}/{1}_By_Mohd_Alrefai'.format(destinationPath, exportName), image='svg')           #offline
    if online:
        print('Open the Graph Online.....')
        py.plot(fig, filename='{0}_By_Mohd_Alrefai'.format(exportName))                 #online

    
    # Save the figure as a png image from online:
    #print('Save the Graph.....')
    #image.save_as(fig, '{0}/{1}.{2}'.format(destinationPath, exportName, 'png'))
    
   


def scatterPlot(destinationPath, scaFile, headerS=True):

    X=[]
    Y=[]
    labels=[]
    with openFile(destinationPath, scaFile) as lines:
        for line in lines:
            if headerS:
                headerS = False
                continue
            tmp = line.split(',')
            x=int(tmp[1].strip())
            l=tmp[2].strip()
            y=int(tmp[3].strip())
            X.append(x)
            Y.append(y)
            labels.append(l)
    
    lines.close()
    return (X,Y,labels)


def drawScatterCollor2D(scatterData, online):
    
    scTitle='<b>{}</b>'.format(scatterData[0][0])
    xaxisTitle=scatterData[0][1]
    yaxisTitle=scatterData[0][2]
    exportName=scatterData[0][3]
    destinationPath=scatterData[0][4]
    scTitle = '<br />'.join(scTitle.split('\n'))

    X=scatterData[1][0]
    Y=scatterData[1][1]
    labels=scatterData[1][2]

    trace1 = Scatter(x=X,
                     y=Y,
                     name='thirParty',
                     text=labels,
                     mode='markers',                     
                     marker=dict(size='10',
                                 color=Y,                  #set color equal to a variable
                                 colorscale='Viridis', 
                                 showscale=True))

    layout = dict(title = scTitle,
                  yaxis = dict(zeroline = False, title=yaxisTitle),
                  xaxis = dict(zeroline = False, title=xaxisTitle),
                  hovermode='closest')
   
    data=Data([trace1])

    print('Draw the Scatter Plot.....')
    fig=Figure(data=data, layout=layout)
    
    print('Open the Scatter Plot.....')
    plot(fig, filename='{0}/{1}_By_Mohd_Alrefai'.format(destinationPath, exportName), image='png')           #offline
    if online:
        print('Open the Scatter Plot Online.....')
        py.plot(fig, filename='{0}_By_Mohd_Alrefai'.format(exportName))                 #online

    # Save the figure as a png image from online:
    #print('Save the  Scatter Plot.....')
    #py.image.save_as(fig, '{0}/{1}.{2}'.format(destinationPath, exportName, 'png'))


def setupPlotly(dData):
    plotly.tools.set_credentials_file(username=dData['username'], api_key=dData['api_key'])
    plotly.tools.set_config_file(plotly_domain=dData['plotly_domain'],
                             plotly_streaming_domain=dData['plotly_streaming_domain'], 
                             plotly_api_domain=dData['plotly_api_domain'],
                             sharing=dData['sharing'],
                             world_readable=dData['world_readable'],
                             plotly_ssl_verification=dData['plotly_ssl_verification'],
                             plotly_proxy_authorization=dData['plotly_proxy_authorization'],
                             auto_open=dData['auto_open'])


def main(destinationPath, files, online=False):
 
    if online:
        dData={}
        dData['username']='malrefai' 
        dData['api_key']='6r9rx3wc98'
        dData['plotly_domain']='https://plot.ly'
        dData['plotly_streaming_domain']='stream.plot.ly'
        dData['plotly_api_domain']='https://api.plot.ly'
        dData['sharing']='public'
        dData['world_readable']=True
        dData['plotly_ssl_verification']=True
        dData['plotly_proxy_authorization']=False
        dData['auto_open']=True
        setupPlotly(dData)

    #(Title, xTitle, yTitle, exportName)  #Q1
    title='Scatter Plot\n(Third Party HTTP Request)'
    xTitle='URL Rank Accordint to Alexa'
    yTitle='Number of Third Party HTTP Request'
    exportName='ThirdPartyHttpRequest'
    #scData = ((title, xTitle, yTitle, exportName, destinationPath), scatterPlot(destinationPath, files[2]))
    #drawScatterCollor2D(scData, online)


    #(Title, xTitle, yTitle, exportName)  #Q2
    title='Scatter Plot\n(Third Party Cookie)'
    xTitle='URL Rank Accordint to Alexa'
    yTitle='Number of Third Party Cookie'
    exportName='ThirdPartyCookie'
    #scData = ((title, xTitle, yTitle, exportName, destinationPath), scatterPlot(destinationPath, files[3]))
    #drawScatterCollor2D(scData, online)
    
    #(Title, exportName)  #Q3
    title='Graph 3D\n(Third Party 3D Visualization)'
    exportName='ThirdPartyGraph3D'
    graphData = ((title, exportName, destinationPath), graphAnalysis(destinationPath, files[0], files[1]))     
    drawGraph3D(graphData, online)



destinationPath  = './files'
gVerticesFiles = 'httpRequestDomainVertex.csv'
gEdgesFiles = 'httpRequestDomainEdge.csv'
scThirdRequest = 'distinctNumberThirdRequest.csv'
scThirdCookies = 'distinctNumberThirdCookie.csv'
files= (gVerticesFiles, gEdgesFiles, scThirdRequest, scThirdCookies)
main(destinationPath, files, online=False)

