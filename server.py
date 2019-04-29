import Node
import GrenadeGame

broker_addr = 'ec2-34-207-68-81.compute-1.amazonaws.com:9092'

server = Node.Node('server', broker_addr, 0, 'spectator')

GrenadeGame.GrenadeGame(server,)








