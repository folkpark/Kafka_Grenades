import Node
import GrenadeGame

broker_addr = 'ec2-34-207-68-81.compute-1.amazonaws.com:9092'

client = Node.Node('client', broker_addr, 0, '0,0')

game = GrenadeGame.GrenadeGame(client,)

