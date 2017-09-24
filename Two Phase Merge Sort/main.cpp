#include<bits/stdc++.h>

using namespace std;
int record_size=0, total_coloumns=0, no_of_record=0, no_of_file=0, no_records_in_phase=0, block_size=0;
std::vector<int> indexes_to_sort;
std::vector< pair <int, int> > pntr;
vector< vector< string> > final_block;
vector< vector< string> >::iterator pit;
string order;
typedef pair<string, int> P;


vector<string> str_split(string a,string delim){
	string s = a;
	string delimiter = delim;

	vector<string> peices;

	size_t pos = 0;
	string token;
	while ((pos = s.find(delimiter)) != -1)
	{
		token = s.substr(0, pos);
		peices.push_back(token);
		s.erase(0, pos + delimiter.length());
	}
	peices.push_back(s);
	return peices;
}
class Order	{
public:
	bool operator()(P const& A, P const& B) const
	{
		string a=A.first,b=B.first;
	  int i=0,l=indexes_to_sort.size(),ind;
	  while(1) {
	    if(i==l)  return false;
			ind=indexes_to_sort[i];
	    string aa=a.substr(pntr[ind].first, pntr[ind].first+pntr[ind].second);
			string bb=b.substr(pntr[ind].first, pntr[ind].first+pntr[ind].second);
	    if(aa<bb) return false;
	    else if (aa>bb) return true;
	    else i++;
	  }
	}
};


bool CompareIt(string a, string b) {
  int i=0,l=indexes_to_sort.size(),ind;
  while(1) {
    if(i==l)  return true; ind=indexes_to_sort[i];
    string aa=a.substr(pntr[ind].first, pntr[ind].first+pntr[ind].second), bb=b.substr(pntr[ind].first, pntr[ind].first+pntr[ind].second);
    if(aa<bb) return true;
    else if (aa>bb) return false;
    else i++;
  }
}

map< string, int> metadata_read() {
  map< string, int> md;
  vector<string> fields;
  ifstream file;
  string line;
  file.open("metadata.txt");

  if(file.is_open()) {
    while(getline(file, line)) {
      fields = str_split(line, ",");
      md[fields[0]] = atoi(fields[1].c_str());
    }
    file.close();
    return md;
  }
  perror("Unable to open metadata.txt");
  exit(0);
}

void sort_chunks(string input_file) {
  int records=0,page = 0,i,t;
  vector<string> chunk;
  string line,name;
  ifstream ifile(input_file.c_str());

  if(ifile.is_open()) {
    for(t=0;t<no_of_file;t++){
      for(i=0;i<no_records_in_phase;i++)  {
        if(getline(ifile, line)) chunk.push_back(line);
        else break;
      }

      sort(chunk.begin(),chunk.end(),CompareIt);
      if(order!="asc")  reverse(chunk.begin(), chunk.end());

      string no=to_string(t);
      name = "chunk_" + no + ".txt";
      ofstream outfile(name.c_str());

			records = chunk.size();
      for(i=0;i<records;i++) outfile << chunk[i] << endl;
      chunk.clear();
			outfile.close();
    }
  }
  else {
    perror("File can't be opened"); exit(0);
  }
return ;
}

int main (int argc, char *argv[]) {

	// Checking Arguments are proper
  if(argc<6){
    perror("Invalid Input!!!"); exit(0);
  }

	// Initializing Variables
  int memory=(2*atoi(argv[3])*1024*1024)/3;
	order=argv[4];
  string input_file=argv[1], output_file=argv[2],line;
  map< string, int> metadata = metadata_read();
  map< string, int>::iterator it;

	// Processing Metadata
  for(it=metadata.begin();it!=metadata.end();it++) {
    pntr.push_back(make_pair(record_size, it->second));
    record_size += it->second+2;
  }

	// Find coloumns to sort
  total_coloumns = metadata.size();
  int i,j;
  for(i=5;i<argc;i++) {
    j=0;
    for(it=metadata.begin();it!=metadata.end();it++) {
      if(argv[i]==it->first) {
        indexes_to_sort.push_back(j); break;
      }
      j++;
    }
  }

	// Finding No of records in file
  ifstream infile(input_file.c_str());
  if(infile.is_open()) {
    while(getline(infile, line))  no_of_record++;
    infile.close();
  }
  else {
    perror("Unable to open input file sorry\n");
    exit(0);
  }

	// Calculating Block size can be taken
  no_records_in_phase = memory/record_size;
  no_of_file = no_of_record/no_records_in_phase;
	if(no_of_record%no_records_in_phase!=0)	no_of_file+=1;
  block_size = memory/((no_of_file+1)*record_size);

  if(block_size<1){
    perror("Data can't be sort with given memory");
    exit(0);
  }
  else	sort_chunks(input_file);

	// Merge Chunks
	vector< ifstream* > files;
	int file_ended[no_of_file+1]={0};
	string name;
	final_block.resize(no_of_file+1);
	for(i=0;i<no_of_file;i++) {
		name = to_string(i); name = "chunk_" + name + ".txt";
		ifstream *tmp = new ifstream(name.c_str());

		files.push_back(tmp);
		for(j=0;j<block_size;j++) {
			if(getline(*files[i], line))
				final_block[i].push_back(line);
			else {
				file_ended[i]=1;
				break;
			}
		}
		reverse(final_block[i].begin(), final_block[i].end());
	}

	priority_queue< P, vector<P>, Order > pq;

	for(i=0;i<no_of_file;i++) {
		pq.push(make_pair(final_block[i].back(),i));
		final_block[i].pop_back();
	}



	ofstream to_out(output_file.c_str());
	P tm;

	//cout << final_block[8].size() << " " << final_block[7].size() << " size is " << endl;

	while(!pq.empty()) {
		tm = pq.top(); pq.pop();
		to_out << tm.first << endl;

		if(final_block[tm.second].empty()){
			for(j=0;j<block_size;j++)	if(getline(*files[tm.second], line))	final_block[tm.second].push_back(line);
			reverse(final_block[tm.second].begin(),final_block[tm.second].end());
		}
		if(!final_block[tm.second].empty()){
			pq.push(make_pair(final_block[tm.second].back(),tm.second));
			final_block[tm.second].pop_back();
		}
	}
	to_out.close();
return 0;
}


/*
for(i=0;i<no_of_file;i++) {
	pq.push(make_pair(final_block[i].back(),i));
	//final_block[i].pop_back();
}
P tm1;
for(i=0;i<no_of_file;i++) {
	tm1 = pq.top();
	cout << tm1.first << endl;
	cout << tm1.second << endl;
	pq.pop();
}
*/
